mod insert_exec;
mod lookup_exec;
mod scan_exec;

use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, Int8Builder, RecordBatch, StringBuilder,
};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::ScalarValue;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use fluss::client::FlussConnection;
use fluss::metadata::{DataType as FlussDataType, TableInfo};
use fluss::record::to_arrow_schema;
use fluss::row::{GenericRow, InternalRow};
use futures::StreamExt;

use crate::error::fluss_err;
pub use insert_exec::FlussInsertExec;
pub use lookup_exec::FlussLookupExec;
pub use scan_exec::FlussScanExec;

pub(crate) fn to_fluss_err(msg: String) -> fluss::error::Error {
    fluss::error::Error::UnexpectedError {
        message: msg,
        source: None,
    }
}

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
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.table_info.has_primary_key() {
            if let Some((pk_index, pk_literal)) = extract_pk_eq_literal(filters, &self.table_info) {
                return Ok(Arc::new(FlussLookupExec::new(
                    self.conn.clone(),
                    self.table_info.clone(),
                    self.arrow_schema.clone(),
                    projection.cloned(),
                    pk_index,
                    pk_literal,
                )));
            }
        }

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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        if !self.table_info.has_primary_key() {
            return Ok(filters
                .iter()
                .map(|_| TableProviderFilterPushDown::Unsupported)
                .collect());
        }

        Ok(filters
            .iter()
            .map(|f| {
                if is_pk_eq_literal_filter(f, &self.table_info) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

fn primary_key_info(table_info: &TableInfo) -> Option<(usize, &str)> {
    let pk = table_info.schema.primary_key()?;
    if pk.column_names().len() != 1 {
        return None;
    }
    let name = pk.column_names()[0].as_str();
    let idx = table_info
        .schema
        .columns()
        .iter()
        .position(|c| c.name() == name)?;
    Some((idx, name))
}

fn parse_eq_col_literal(expr: &Expr) -> Option<(String, ScalarValue)> {
    let Expr::BinaryExpr(be) = expr else {
        return None;
    };
    if be.op != Operator::Eq {
        return None;
    }

    match (unwrap_col_expr(&be.left), unwrap_lit_expr(&be.right)) {
        (Some(c), Some(v)) => Some((c, v)),
        _ => match (unwrap_lit_expr(&be.left), unwrap_col_expr(&be.right)) {
            (Some(v), Some(c)) => Some((c, v)),
            _ => None,
        },
    }
}

fn unwrap_col_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(c) => Some(c.name.clone()),
        Expr::Cast(cast) => unwrap_col_expr(&cast.expr),
        Expr::TryCast(cast) => unwrap_col_expr(&cast.expr),
        _ => None,
    }
}

fn unwrap_lit_expr(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(v, _) => Some(v.clone()),
        Expr::Cast(cast) => unwrap_lit_expr(&cast.expr),
        Expr::TryCast(cast) => unwrap_lit_expr(&cast.expr),
        _ => None,
    }
}

fn col_eq(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

fn is_pk_eq_literal_filter(expr: &Expr, table_info: &TableInfo) -> bool {
    let Some((_, pk_name)) = primary_key_info(table_info) else {
        return false;
    };
    let Some((col, _)) = parse_eq_col_literal(expr) else {
        return false;
    };
    col_eq(&col, pk_name)
}

fn extract_pk_eq_literal(filters: &[Expr], table_info: &TableInfo) -> Option<(usize, ScalarValue)> {
    let (pk_index, pk_name) = primary_key_info(table_info)?;
    for f in filters {
        let Some((col, lit)) = parse_eq_col_literal(f) else {
            continue;
        };
        if col_eq(&col, pk_name) {
            return Some((pk_index, lit));
        }
    }
    None
}

pub(crate) async fn scan_table(
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
    scan_table_with_log_scanner_limit(conn, table_info, projection, limit).await
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

fn append_row_to_builders(
    builders: &mut [ColBuilder],
    row: &dyn InternalRow,
    row_indices: &[usize],
) {
    for (b, idx) in builders.iter_mut().zip(row_indices.iter().copied()) {
        b.append_from_row(row, idx);
    }
}

fn projected_indices(table_info: &TableInfo, projection: Option<&[usize]>) -> Vec<usize> {
    match projection {
        Some(v) => v.to_vec(),
        None => (0..table_info.row_type.fields().len()).collect(),
    }
}

fn set_pk_key_from_scalar(
    key: &mut GenericRow<'_>,
    data_type: &FlussDataType,
    literal: &ScalarValue,
) -> std::result::Result<(), fluss::error::Error> {
    match (data_type, literal) {
        (FlussDataType::TinyInt(_), ScalarValue::Int8(Some(v))) => key.set_field(0, *v),
        (FlussDataType::SmallInt(_), ScalarValue::Int16(Some(v))) => key.set_field(0, *v),
        (FlussDataType::Int(_), ScalarValue::Int32(Some(v))) => key.set_field(0, *v),
        (FlussDataType::BigInt(_), ScalarValue::Int64(Some(v))) => key.set_field(0, *v),
        (FlussDataType::Char(_), ScalarValue::Utf8(Some(v)))
        | (FlussDataType::String(_), ScalarValue::Utf8(Some(v))) => key.set_field(0, v.clone()),
        (FlussDataType::Char(_), ScalarValue::LargeUtf8(Some(v)))
        | (FlussDataType::String(_), ScalarValue::LargeUtf8(Some(v))) => {
            key.set_field(0, v.clone())
        }
        _ => {
            return Err(to_fluss_err(format!(
                "PK literal type mismatch, pk_type={data_type:?}, literal={literal:?}"
            )));
        }
    }
    Ok(())
}

pub(crate) async fn lookup_pk_row(
    conn: &FlussConnection,
    table_info: &TableInfo,
    projection: Option<&[usize]>,
    pk_index: usize,
    pk_literal: &ScalarValue,
) -> std::result::Result<Vec<RecordBatch>, fluss::error::Error> {
    let metadata = conn.get_metadata();
    let mut lookup_table_info = table_info.clone();
    lookup_table_info.properties.remove("table.datalake.format");
    let table = fluss::client::FlussTable::new(conn, metadata, lookup_table_info.clone());
    let table_lookup = table.new_lookup().map_err(|e| {
        to_fluss_err(format!(
            "lookup step failed: table.new_lookup() on {}: {e:?}",
            table_info.table_path
        ))
    })?;
    let mut lookuper = table_lookup.create_lookuper().map_err(|e| {
        to_fluss_err(format!(
            "lookup step failed: create_lookuper() on {}: {e:?}",
            table_info.table_path
        ))
    })?;

    let pk_data_type = table_info.schema.columns()[pk_index].data_type().clone();
    let mut key = GenericRow::new(1);
    set_pk_key_from_scalar(&mut key, &pk_data_type, pk_literal).map_err(|e| {
        to_fluss_err(format!(
            "lookup step failed: set_pk_key_from_scalar() pk_type={pk_data_type:?}, literal={pk_literal:?}: {e:?}"
        ))
    })?;

    let result = lookuper.lookup(&key).await.map_err(|e| {
        to_fluss_err(format!(
            "lookup step failed: lookuper.lookup() on {}: {e:?}",
            table_info.table_path
        ))
    })?;
    let Some(row) = result.get_single_row().map_err(|e| {
        to_fluss_err(format!(
            "lookup step failed: get_single_row() on {}: {e:?}",
            table_info.table_path
        ))
    })?
    else {
        return Ok(vec![]);
    };

    let indices = projected_indices(table_info, projection);
    let projected_fields: Vec<&fluss::metadata::DataField> = indices
        .iter()
        .map(|&i| &table_info.row_type.fields()[i])
        .collect();
    let mut builders = make_builders(&projected_fields, 1).map_err(|e| {
        to_fluss_err(format!(
            "lookup step failed: make_builders() projected_fields={:?}: {e:?}",
            projected_fields
                .iter()
                .map(|f| format!("{}:{:?}", f.name(), f.data_type()))
                .collect::<Vec<_>>()
        ))
    })?;
    append_row_to_builders(&mut builders, &row, &indices);

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

pub(crate) async fn upsert_batches(
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
