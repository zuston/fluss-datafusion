//! Terminal-friendly formatting for query result batches.
//!
//! Arrow's default pretty printer puts entire `List<Struct<...>>` values on one line,
//! which is hard to read. For nested column types we serialize each cell to JSON and
//! pretty-print it so the REPL output breaks across lines inside the cell.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Array, ArrayRef, StringBuilder};
use arrow::json::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use serde_json::Value;

/// True when a column is likely to produce very wide one-line Arrow display output.
fn wants_pretty_json(dt: &DataType) -> bool {
    match dt {
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _)
        | DataType::Union(_, _) => true,
        DataType::Dictionary(_, v) => wants_pretty_json(v),
        _ => false,
    }
}

fn batch_needs_transform(batch: &RecordBatch) -> bool {
    batch
        .schema()
        .fields()
        .iter()
        .any(|f| wants_pretty_json(f.data_type()))
}

fn column_row_to_pretty_json(field: &Field, col: &dyn Array, row: usize) -> Result<String> {
    let sliced = col.slice(row, 1);
    let mini_schema = Arc::new(Schema::new(vec![field.clone()]));
    let mini = RecordBatch::try_new(mini_schema, vec![sliced]).context("mini RecordBatch")?;

    let mut buf = Vec::new();
    {
        let mut w = LineDelimitedWriter::new(&mut buf);
        w.write(&mini).context("arrow json write")?;
        w.finish().context("arrow json finish")?;
    }

    let line = String::from_utf8(buf).context("json utf-8")?;
    let line = line.trim();
    if line.is_empty() {
        return Ok(String::new());
    }

    let value: Value = serde_json::from_str(line).context("parse json line")?;
    let inner = match value {
        Value::Object(map) if map.len() == 1 => map.into_values().next().unwrap(),
        other => other,
    };
    serde_json::to_string_pretty(&inner).context("pretty json")
}

fn build_pretty_json_string_col(
    field: &Field,
    col: &dyn Array,
) -> Result<arrow::array::StringArray> {
    let mut b = StringBuilder::with_capacity(col.len(), col.len().saturating_mul(256));
    for row in 0..col.len() {
        if col.is_null(row) {
            b.append_null();
        } else {
            let s = column_row_to_pretty_json(field, col, row)
                .unwrap_or_else(|e| format!("<nested value: json pretty-print failed: {e}>"));
            b.append_value(&s);
        }
    }
    Ok(b.finish())
}

fn transform_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut new_fields = Vec::with_capacity(schema.fields().len());
    let mut new_cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        if wants_pretty_json(field.data_type()) {
            new_fields.push(Field::new(
                field.name(),
                DataType::Utf8,
                field.is_nullable(),
            ));
            new_cols.push(Arc::new(build_pretty_json_string_col(field, col.as_ref())?) as ArrayRef);
        } else {
            new_fields.push(field.as_ref().clone());
            new_cols.push(col.clone());
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_cols).context("RecordBatch::try_new after nested display")
}

/// Clone batches, rewriting nested columns to multi-line JSON strings for terminal display.
pub fn prepare_batches_for_terminal(batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
    let mut out = Vec::with_capacity(batches.len());
    for b in batches {
        if batch_needs_transform(b) {
            out.push(transform_batch(b)?);
        } else {
            out.push(b.clone());
        }
    }
    Ok(out)
}
