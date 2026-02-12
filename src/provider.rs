use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, Int8Builder, RecordBatch, StringBuilder, UInt64Array,
};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

use fluss::client::FlussConnection;
use fluss::metadata::{DataType as FlussDataType, TableInfo};
use fluss::record::to_arrow_schema;
use fluss::row::{GenericRow, InternalRow};

use crate::error::fluss_err;

fn to_fluss_err(msg: String) -> fluss::error::Error {
    fluss::error::Error::UnexpectedError {
        message: msg,
        source: None,
    }
}

// ---------------------------------------------------------------------------
// FlussTableProvider
// ---------------------------------------------------------------------------

pub struct FlussTableProvider {
    conn: Arc<FlussConnection>,
    table_info: TableInfo,
    arrow_schema: SchemaRef,
}

impl Debug for FlussTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "FlussTable({})", self.table_info.table_path)
    }
}

impl FlussTableProvider {
    pub fn new(conn: Arc<FlussConnection>, table_info: TableInfo) -> Result<Self> {
        let arrow_schema = to_arrow_schema(&table_info.row_type).map_err(fluss_err)?;
        Ok(Self {
            conn,
            table_info,
            arrow_schema,
        })
    }
}

#[async_trait]
impl TableProvider for FlussTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FlussScanExec::new(
            self.conn.clone(),
            self.table_info.clone(),
            self.arrow_schema.clone(),
            projection.cloned(),
            limit,
        )))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FlussInsertExec::new(
            self.conn.clone(),
            self.table_info.clone(),
            input,
        )))
    }
}

// ---------------------------------------------------------------------------
// FlussScanExec – scan changelog from earliest→latest, with optional limit.
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct FlussScanExec {
    conn: Arc<FlussConnection>,
    table_info: TableInfo,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    props: PlanProperties,
}

impl FlussScanExec {
    pub fn new(
        conn: Arc<FlussConnection>,
        table_info: TableInfo,
        arrow_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        let projected_schema = match &projection {
            Some(indices) => Arc::new(arrow_schema.project(indices).unwrap()),
            None => arrow_schema,
        };
        let props = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            conn,
            table_info,
            projection,
            limit,
            props,
        }
    }

    fn projected_schema(&self) -> SchemaRef {
        self.props.equivalence_properties().schema().clone()
    }
}

impl Debug for FlussScanExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlussScanExec")
            .field("table", &self.table_info.table_path.to_string())
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .finish()
    }
}

impl DisplayAs for FlussScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "FlussScanExec: table={}, limit={:?}",
            self.table_info.table_path, self.limit
        )
    }
}

impl ExecutionPlan for FlussScanExec {
    fn name(&self) -> &'static str {
        "FlussScanExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.props
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let conn = self.conn.clone();
        let table_info = self.table_info.clone();
        let projection = self.projection.clone();
        let limit = self.limit;
        let projected_schema = self.projected_schema();

        let batches: Vec<RecordBatch> = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                scan_table(&conn, &table_info, projection.as_deref(), limit)
                    .await
                    .unwrap_or_default()
            })
        });

        Ok(Box::pin(MemoryStream::try_new(
            batches,
            projected_schema,
            None,
        )?))
    }
}

// ---------------------------------------------------------------------------
// Scan implementation: read changelog from earliest to latest offset.
// ---------------------------------------------------------------------------

async fn scan_table(
    conn: &FlussConnection,
    table_info: &TableInfo,
    projection: Option<&[usize]>,
    limit: Option<usize>,
) -> std::result::Result<Vec<RecordBatch>, fluss::error::Error> {
    if limit.is_none() {
        return Err(fluss::error::Error::IllegalArgument {
            message: "must be with LIMIT".to_string(),
        });
    }
    match scan_table_with_log_scanner_limit(conn, table_info, projection, limit).await {
        Ok(batches) => Ok(batches),
        Err(e) => {
            println!("scan error: {:?}", e);
            Err(e)
        }
    }
}

enum ColBuilder {
    Bool(BooleanBuilder),
    I8(Int8Builder),
    I16(Int16Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Utf8(StringBuilder),
}

impl ColBuilder {
    fn append_from_row(&mut self, row: &dyn InternalRow, idx: usize) {
        if row.is_null_at(idx) {
            match self {
                ColBuilder::Bool(b) => b.append_null(),
                ColBuilder::I8(b) => b.append_null(),
                ColBuilder::I16(b) => b.append_null(),
                ColBuilder::I32(b) => b.append_null(),
                ColBuilder::I64(b) => b.append_null(),
                ColBuilder::F32(b) => b.append_null(),
                ColBuilder::F64(b) => b.append_null(),
                ColBuilder::Utf8(b) => b.append_null(),
            }
            return;
        }

        match self {
            ColBuilder::Bool(b) => b.append_value(row.get_boolean(idx)),
            ColBuilder::I8(b) => b.append_value(row.get_byte(idx)),
            ColBuilder::I16(b) => b.append_value(row.get_short(idx)),
            ColBuilder::I32(b) => b.append_value(row.get_int(idx)),
            ColBuilder::I64(b) => b.append_value(row.get_long(idx)),
            ColBuilder::F32(b) => b.append_value(row.get_float(idx)),
            ColBuilder::F64(b) => b.append_value(row.get_double(idx)),
            ColBuilder::Utf8(b) => b.append_value(row.get_string(idx)),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            ColBuilder::Bool(mut b) => Arc::new(b.finish()),
            ColBuilder::I8(mut b) => Arc::new(b.finish()),
            ColBuilder::I16(mut b) => Arc::new(b.finish()),
            ColBuilder::I32(mut b) => Arc::new(b.finish()),
            ColBuilder::I64(mut b) => Arc::new(b.finish()),
            ColBuilder::F32(mut b) => Arc::new(b.finish()),
            ColBuilder::F64(mut b) => Arc::new(b.finish()),
            ColBuilder::Utf8(mut b) => Arc::new(b.finish()),
        }
    }
}

fn projected_fields<'a>(
    table_info: &'a TableInfo,
    projection: Option<&[usize]>,
) -> Vec<&'a fluss::metadata::DataField> {
    let fields = table_info.row_type.fields();
    match projection {
        Some(idxs) => idxs.iter().map(|&i| &fields[i]).collect(),
        None => fields.iter().collect(),
    }
}

fn make_builders(
    fields: &[&fluss::metadata::DataField],
    capacity: usize,
) -> std::result::Result<Vec<ColBuilder>, fluss::error::Error> {
    let mut out = Vec::with_capacity(fields.len());
    for f in fields {
        let b = match f.data_type() {
            FlussDataType::Boolean(_) => ColBuilder::Bool(BooleanBuilder::with_capacity(capacity)),
            FlussDataType::TinyInt(_) => ColBuilder::I8(Int8Builder::with_capacity(capacity)),
            FlussDataType::SmallInt(_) => ColBuilder::I16(Int16Builder::with_capacity(capacity)),
            FlussDataType::Int(_) => ColBuilder::I32(Int32Builder::with_capacity(capacity)),
            FlussDataType::BigInt(_) => ColBuilder::I64(Int64Builder::with_capacity(capacity)),
            FlussDataType::Float(_) => ColBuilder::F32(Float32Builder::with_capacity(capacity)),
            FlussDataType::Double(_) => ColBuilder::F64(Float64Builder::with_capacity(capacity)),
            FlussDataType::Char(_) | FlussDataType::String(_) => {
                ColBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 8))
            }
            other => {
                return Err(to_fluss_err(format!(
                    "unsupported PK limit scan type: {other:?}"
                )));
            }
        };
        out.push(b);
    }
    Ok(out)
}

async fn scan_table_with_log_scanner_limit(
    conn: &FlussConnection,
    table_info: &TableInfo,
    projection: Option<&[usize]>,
    limit: Option<usize>,
) -> std::result::Result<Vec<RecordBatch>, fluss::error::Error> {
    use fluss::rpc::message::OffsetSpec;

    let lim = limit.unwrap_or(0);
    if lim == 0 {
        return Ok(vec![]);
    }

    let admin = conn.get_admin().await?;
    let bucket_ids: Vec<i32> = (0..table_info.num_buckets).collect();
    let latest = admin
        .list_offsets(&table_info.table_path, &bucket_ids, OffsetSpec::Latest)
        .await?;

    let mut subscribe_offsets = std::collections::HashMap::new();
    for &b in &bucket_ids {
        subscribe_offsets.insert(b, latest.get(&b).copied().unwrap_or(0));
    }

    let table = conn.get_table(&table_info.table_path).await?;
    let mut scan = table.new_scan();
    if let Some(cols) = projection {
        scan = scan.project(cols)?;
    }
    let scanner = scan.create_log_scanner()?;
    scanner.subscribe_buckets(&subscribe_offsets).await?;

    let fields = projected_fields(table_info, projection);
    let mut builders = make_builders(&fields, lim)?;
    let mut collected = 0usize;
    let mut empty_polls = 0usize;

    // Simple strategy: poll until enough rows or several empty polls.
    while collected < lim && empty_polls < 3 {
        let records = scanner.poll(Duration::from_secs(2)).await?;
        if records.is_empty() {
            empty_polls += 1;
            continue;
        }
        empty_polls = 0;

        for rec in records {
            let row = rec.row();
            for (i, b) in builders.iter_mut().enumerate() {
                b.append_from_row(row, i);
            }
            collected += 1;
            if collected >= lim {
                break;
            }
        }
    }

    if collected == 0 {
        return Ok(vec![]);
    }

    let arrays: Vec<ArrayRef> = builders.into_iter().map(ColBuilder::finish).collect();
    let schema = match projection {
        Some(idxs) => Arc::new(
            to_arrow_schema(&table_info.row_type)?
                .project(idxs)
                .map_err(|e| to_fluss_err(format!("project schema failed: {e}")))?,
        ),
        None => to_arrow_schema(&table_info.row_type)?,
    };
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|e| to_fluss_err(format!("build record batch failed: {e}")))?;
    Ok(vec![batch])
}

// ---------------------------------------------------------------------------
// FlussInsertExec – upsert into Fluss PK table.
// ---------------------------------------------------------------------------

pub struct FlussInsertExec {
    conn: Arc<FlussConnection>,
    table_info: TableInfo,
    input: Arc<dyn ExecutionPlan>,
    props: PlanProperties,
}

impl FlussInsertExec {
    pub fn new(
        conn: Arc<FlussConnection>,
        table_info: TableInfo,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let count_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            ArrowDataType::UInt64,
            false,
        )]));
        let props = PlanProperties::new(
            EquivalenceProperties::new(count_schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            conn,
            table_info,
            input,
            props,
        }
    }
}

impl Debug for FlussInsertExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "FlussInsertExec: table={}", self.table_info.table_path)
    }
}

impl DisplayAs for FlussInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "FlussInsertExec: table={}", self.table_info.table_path)
    }
}

impl ExecutionPlan for FlussInsertExec {
    fn name(&self) -> &'static str {
        "FlussInsertExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.props
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FlussInsertExec::new(
            self.conn.clone(),
            self.table_info.clone(),
            children.remove(0),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let conn = self.conn.clone();
        let table_info = self.table_info.clone();
        let input = self.input.clone();
        let count_schema = self.props.equivalence_properties().schema().clone();

        let total_rows: u64 = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                upsert_batches(&conn, &table_info, input, partition, context)
                    .await
                    .unwrap_or(0)
            })
        });

        let count_batch = RecordBatch::try_new(
            count_schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![total_rows]))],
        )?;
        Ok(Box::pin(MemoryStream::try_new(
            vec![count_batch],
            count_schema,
            None,
        )?))
    }
}

/// Upsert all batches from the input plan into the Fluss table.
async fn upsert_batches(
    conn: &FlussConnection,
    table_info: &TableInfo,
    input: Arc<dyn ExecutionPlan>,
    partition: usize,
    context: Arc<TaskContext>,
) -> std::result::Result<u64, fluss::error::Error> {
    let table = conn.get_table(&table_info.table_path).await?;
    let writer = table.new_upsert()?.create_writer()?;

    let schema = table_info.row_type.fields();
    let mut stream = input
        .execute(partition, context)
        .map_err(|e| to_fluss_err(e.to_string()))?;

    let mut total: u64 = 0;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(|e| to_fluss_err(e.to_string()))?;
        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            let mut row = GenericRow::new(schema.len());
            for (col_idx, field) in schema.iter().enumerate() {
                set_generic_row_from_batch(&mut row, col_idx, &batch, row_idx, field);
            }
            writer.upsert(&row)?;
        }
        total += num_rows as u64;
    }
    writer.flush().await?;
    Ok(total)
}

/// Extract a value from a RecordBatch column and set it on a GenericRow.
fn set_generic_row_from_batch(
    row: &mut GenericRow<'_>,
    col_idx: usize,
    batch: &RecordBatch,
    row_idx: usize,
    field: &fluss::metadata::DataField,
) {
    use arrow::array::*;
    use fluss::metadata::DataType as FlussDataType;

    let col = batch.column(col_idx);
    if col.is_null(row_idx) {
        return;
    }

    match field.data_type() {
        FlussDataType::Boolean(_) => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            row.set_field(col_idx, arr.value(row_idx));
        }
        FlussDataType::TinyInt(_) => {
            let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
            row.set_field(col_idx, arr.value(row_idx));
        }
        FlussDataType::SmallInt(_) => {
            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
            row.set_field(col_idx, arr.value(row_idx));
        }
        FlussDataType::Int(_) => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            row.set_field(col_idx, arr.value(row_idx));
        }
        FlussDataType::BigInt(_) => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            row.set_field(col_idx, arr.value(row_idx));
        }
        FlussDataType::Float(_) => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
            row.set_field(col_idx, arr.value(row_idx));
        }
        FlussDataType::Double(_) => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            row.set_field(col_idx, arr.value(row_idx));
        }
        FlussDataType::Char(_) | FlussDataType::String(_) => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            row.set_field(col_idx, arr.value(row_idx).to_owned());
        }
        _ => {
            log::warn!(
                "Unsupported type for column {}: {:?}, skipping",
                col_idx,
                field.data_type()
            );
        }
    }
}
