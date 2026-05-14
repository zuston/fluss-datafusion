mod insert_exec;
mod lookup_exec;
mod scan_exec;

use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
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
use fluss::record::{to_arrow_schema, RowAppendRecordBatchBuilder};
use fluss::row::GenericRow;
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

    let batch = result.to_record_batch().map_err(|e| {
        to_fluss_err(format!(
            "lookup step failed: to_record_batch() on {}: {e:?}",
            table_info.table_path
        ))
    })?;

    if batch.num_rows() == 0 {
        return Ok(vec![]);
    }
    if batch.num_rows() != 1 {
        return Err(to_fluss_err(format!(
            "lookup expected at most one row on {}, got {}",
            table_info.table_path,
            batch.num_rows()
        )));
    }

    let batch = match projection {
        Some(idxs) => batch
            .project(idxs)
            .map_err(|e| to_fluss_err(format!("project batch failed: {e}")))?,
        None => batch,
    };
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

    let admin = conn.get_admin()?;
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

    let row_type_for_builder = match projection {
        Some(idxs) => table_info.row_type.project(idxs).map_err(|e| {
            to_fluss_err(format!(
                "scan failed: row_type.project on {}: {e:?}",
                table_info.table_path
            ))
        })?,
        None => table_info.row_type.clone(),
    };
    let mut builder = RowAppendRecordBatchBuilder::new(&row_type_for_builder).map_err(|e| {
        to_fluss_err(format!(
            "scan failed: RowAppendRecordBatchBuilder::new on {}: {e:?}",
            table_info.table_path
        ))
    })?;
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
            builder.append(rec.row()).map_err(|e| {
                to_fluss_err(format!(
                    "scan failed: append row on {}: {e:?}",
                    table_info.table_path
                ))
            })?;
            collected += 1;
            if collected >= lim {
                break;
            }
        }
    }

    if collected == 0 {
        return Ok(vec![]);
    }

    let batch = builder
        .build_arrow_record_batch()
        .map_err(|e| {
            to_fluss_err(format!(
                "scan failed: build_arrow_record_batch on {}: {e:?}",
                table_info.table_path
            ))
        })
        .map(Arc::unwrap_or_clone)?;
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
