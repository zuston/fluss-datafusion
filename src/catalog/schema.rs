//! Schema Module - Mapping between Fluss Database and DataFusion Schema

use std::any::Any;
use std::sync::Arc;

use arrow::array::{BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::Constraint;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use fluss::client::FlussConnection;
use fluss::metadata::{
    DataType as FlussDataType, DataTypes, Schema as FlussTableSchema, TableDescriptor, TableInfo,
    TablePath,
};

use crate::error::fluss_err;
use crate::provider::FlussTableProvider;
use crate::sql::SqlContext;

/// Fluss Schema - Maps a single Fluss database
pub struct FlussSchema {
    conn: Arc<FlussConnection>,
    database: String,
}

impl std::fmt::Debug for FlussSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlussSchema")
            .field("database", &self.database)
            .finish()
    }
}

impl FlussSchema {
    pub fn new(conn: Arc<FlussConnection>, database: String) -> Self {
        Self { conn, database }
    }
}

#[async_trait]
impl SchemaProvider for FlussSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // Sync context – use block_in_place to query Fluss for table list.
        let conn = self.conn.clone();
        let db = self.database.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                match conn.get_admin() {
                    Ok(admin) => admin.list_tables(&db).await.unwrap_or_default(),
                    Err(_) => vec![],
                }
            })
        })
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let path = TablePath::new(self.database.clone(), name.to_string());
        let admin = self.conn.get_admin().map_err(fluss_err)?;
        let exists = admin.table_exists(&path).await.map_err(fluss_err)?;
        if !exists {
            return Ok(None);
        }
        let table_info = admin.get_table_info(&path).await.map_err(fluss_err)?;
        Ok(Some(Arc::new(FlussTableProvider::new(
            self.conn.clone(),
            table_info,
        )?)))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let descriptor = datafusion_table_to_fluss_descriptor(&table)?;
        let conn = self.conn.clone();
        let database = self.database.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let admin = conn.get_admin().map_err(fluss_err)?;
                let table_path = TablePath::new(database, name);
                admin
                    .create_table(&table_path, &descriptor, false)
                    .await
                    .map_err(fluss_err)?;
                Ok::<(), DataFusionError>(())
            })
        })?;
        Ok(None)
    }

    fn table_exist(&self, name: &str) -> bool {
        let conn = self.conn.clone();
        let path = TablePath::new(self.database.clone(), name.to_string());
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                match conn.get_admin() {
                    Ok(admin) => admin.table_exists(&path).await.unwrap_or(false),
                    Err(_) => false,
                }
            })
        })
    }
}

fn datafusion_table_to_fluss_descriptor(
    table: &Arc<dyn TableProvider>,
) -> DataFusionResult<TableDescriptor> {
    let schema = table.schema();
    let mut schema_builder = FlussTableSchema::builder();
    for field in schema.fields() {
        let mut data_type = arrow_to_fluss_type(field.data_type())?;
        if !field.is_nullable() {
            data_type = data_type.as_non_nullable();
        }
        schema_builder = schema_builder.column(field.name().to_string(), data_type);
    }

    if let Some(constraints) = table.constraints() {
        for constraint in constraints.iter() {
            if let Constraint::PrimaryKey(indices) = constraint {
                let mut pk_columns = Vec::with_capacity(indices.len());
                for idx in indices {
                    let field = schema.field(*idx);
                    pk_columns.push(field.name().to_string());
                }
                schema_builder = schema_builder.primary_key(pk_columns);
                break;
            }
        }
    }

    let schema = match schema_builder.build() {
        Ok(schema) => schema,
        Err(e) => return Err(DataFusionError::Execution(e.to_string())),
    };
    match TableDescriptor::builder().schema(schema).build() {
        Ok(descriptor) => Ok(descriptor),
        Err(e) => Err(DataFusionError::Execution(e.to_string())),
    }
}

fn arrow_to_fluss_type(data_type: &DataType) -> DataFusionResult<FlussDataType> {
    match data_type {
        DataType::Boolean => Ok(DataTypes::boolean()),
        DataType::Int8 | DataType::UInt8 => Ok(DataTypes::tinyint()),
        DataType::Int16 | DataType::UInt16 => Ok(DataTypes::smallint()),
        DataType::Int32 | DataType::UInt32 => Ok(DataTypes::int()),
        DataType::Int64 | DataType::UInt64 => Ok(DataTypes::bigint()),
        DataType::Float32 => Ok(DataTypes::float()),
        DataType::Float64 => Ok(DataTypes::double()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(DataTypes::string()),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            Ok(DataTypes::bytes())
        }
        DataType::Date32 | DataType::Date64 => Ok(DataTypes::date()),
        DataType::Time32(_) | DataType::Time64(_) => Ok(DataTypes::time()),
        DataType::Timestamp(_, _) => Ok(DataTypes::timestamp()),
        DataType::Decimal128(precision, scale) => {
            let scale = u32::try_from(*scale).map_err(|_| {
                DataFusionError::Execution(format!(
                    "negative decimal scale is unsupported: {scale}"
                ))
            })?;
            Ok(DataTypes::decimal(u32::from(*precision), scale))
        }
        DataType::Decimal256(precision, scale) => {
            let scale = u32::try_from(*scale).map_err(|_| {
                DataFusionError::Execution(format!(
                    "negative decimal scale is unsupported: {scale}"
                ))
            })?;
            Ok(DataTypes::decimal(u32::from(*precision), scale))
        }
        other => Err(DataFusionError::Execution(format!(
            "unsupported CREATE TABLE type in Fluss schema: {other}"
        ))),
    }
}

// =============================================================================
// Fluss Information Schema - Extended virtual table support
// =============================================================================

/// Fluss Extended Information Schema Provider
///
/// Provides the following virtual tables:
/// - tables: List all databases and tables
/// - table_ddl: Display table CREATE TABLE statements
/// - columns: Table column info (for DESCRIBE TABLE)
/// - partitions: Table partition info (for SHOW PARTITIONS)
/// - buckets: Table bucket info (for SHOW BUCKETS)
/// - table_options: Table configuration options (for SHOW OPTIONS)
/// - table_stats: Table statistics
pub struct FlussInformationSchema {
    conn: Arc<FlussConnection>,
    ctx: SqlContext,
}

impl FlussInformationSchema {
    pub fn new(conn: Arc<FlussConnection>, ctx: SqlContext) -> Self {
        Self { conn, ctx }
    }

    /// Get all supported virtual table names
    pub fn table_names_list() -> Vec<String> {
        vec![
            "tables".to_string(),
            "table_ddl".to_string(),
            "columns".to_string(),
            "partitions".to_string(),
            "buckets".to_string(),
            "table_options".to_string(),
            "table_stats".to_string(),
        ]
    }
}

impl std::fmt::Debug for FlussInformationSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlussInformationSchema")
            .field("database", &self.ctx.current_database)
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for FlussInformationSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        Self::table_names_list()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        match name.to_ascii_lowercase().as_str() {
            "tables" => self.build_tables_table().await,
            "table_ddl" => self.build_table_ddl_table().await,
            "columns" => self.build_columns_table().await,
            "partitions" => self.build_partitions_table().await,
            "buckets" => self.build_buckets_table().await,
            "table_options" => self.build_table_options_table().await,
            "table_stats" => self.build_table_stats_table().await,
            _ => Ok(None),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        let name = name.to_ascii_lowercase();
        matches!(
            name.as_str(),
            "tables"
                | "table_ddl"
                | "columns"
                | "partitions"
                | "buckets"
                | "table_options"
                | "table_stats"
        )
    }
}

impl FlussInformationSchema {
    /// Build tables virtual table - List all databases and tables
    async fn build_tables_table(&self) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let admin = self.conn.get_admin().map_err(fluss_err)?;
        let databases = admin.list_databases().await.map_err(fluss_err)?;

        let mut table_schemas = Vec::new();
        let mut table_names = Vec::new();
        let mut table_types = Vec::new();

        for database in &databases {
            let tables = admin.list_tables(database).await.map_err(fluss_err)?;
            for table in tables {
                table_schemas.push(database.clone());
                table_names.push(table);
                table_types.push("BASE TABLE".to_string());
            }
        }

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(table_schemas)),
                Arc::new(StringArray::from(table_names)),
                Arc::new(StringArray::from(table_types)),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let mem_table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Some(Arc::new(mem_table)))
    }

    /// Build table_ddl virtual table - Display CREATE TABLE statements
    async fn build_table_ddl_table(&self) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let admin = self.conn.get_admin().map_err(fluss_err)?;
        let databases = admin.list_databases().await.map_err(fluss_err)?;

        let mut table_schemas = Vec::new();
        let mut table_names = Vec::new();
        let mut create_table_sqls = Vec::new();

        for database in &databases {
            let tables = admin.list_tables(database).await.map_err(fluss_err)?;
            for table in tables {
                let path = TablePath::new(database.clone(), table.clone());
                let info = admin.get_table_info(&path).await.map_err(fluss_err)?;

                table_schemas.push(database.clone());
                table_names.push(table);
                create_table_sqls.push(format_create_table_sql(&info));
            }
        }

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("create_table", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(table_schemas)),
                Arc::new(StringArray::from(table_names)),
                Arc::new(StringArray::from(create_table_sqls)),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let mem_table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Some(Arc::new(mem_table)))
    }

    /// Build columns virtual table - For DESCRIBE TABLE command
    async fn build_columns_table(&self) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let admin = self.conn.get_admin().map_err(fluss_err)?;
        let databases = admin.list_databases().await.map_err(fluss_err)?;

        let mut table_schemas = Vec::new();
        let mut table_names = Vec::new();
        let mut column_names = Vec::new();
        let mut ordinal_positions = Vec::new();
        let mut data_types = Vec::new();
        let mut is_nullables = Vec::new();
        let mut column_defaults = Vec::new();
        let mut column_comments = Vec::new();
        let mut is_primary_keys = Vec::<bool>::new();

        for database in &databases {
            let tables = admin.list_tables(database).await.map_err(fluss_err)?;
            for table in tables {
                let path = TablePath::new(database.clone(), table.clone());
                let info = admin.get_table_info(&path).await.map_err(fluss_err)?;

                let pk_columns: std::collections::HashSet<String> = if info.has_primary_key() {
                    info.get_primary_keys().iter().cloned().collect()
                } else {
                    std::collections::HashSet::new()
                };

                for (idx, column) in info.get_schema().columns().iter().enumerate() {
                    table_schemas.push(database.clone());
                    table_names.push(table.clone());
                    column_names.push(column.name().to_string());
                    ordinal_positions.push((idx + 1) as i64);
                    data_types.push(format!("{:?}", column.data_type()));
                    is_nullables.push(if column.data_type().is_nullable() {
                        "YES"
                    } else {
                        "NO"
                    });
                    column_defaults.push("NULL".to_string());
                    column_comments.push(column.comment().unwrap_or("").to_string());
                    is_primary_keys.push(pk_columns.contains(column.name()));
                }
            }
        }

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int64, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("is_nullable", DataType::Utf8, false),
            Field::new("column_default", DataType::Utf8, true),
            Field::new("column_comment", DataType::Utf8, true),
            Field::new("is_primary_key", DataType::Boolean, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(table_schemas)),
                Arc::new(StringArray::from(table_names)),
                Arc::new(StringArray::from(column_names)),
                Arc::new(Int64Array::from(ordinal_positions)),
                Arc::new(StringArray::from(data_types)),
                Arc::new(StringArray::from(is_nullables)),
                Arc::new(StringArray::from(column_defaults)),
                Arc::new(StringArray::from(column_comments)),
                Arc::new(BooleanArray::from(is_primary_keys)),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let mem_table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Some(Arc::new(mem_table)))
    }

    /// Build partitions virtual table - For SHOW PARTITIONS command
    ///
    /// Uses Fluss Admin API `list_partition_infos` to get real partition information
    async fn build_partitions_table(&self) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let admin = self.conn.get_admin().map_err(fluss_err)?;
        let databases = admin.list_databases().await.map_err(fluss_err)?;

        let mut table_schemas = Vec::new();
        let mut table_names = Vec::new();
        let mut partition_ids = Vec::new();
        let mut partition_names = Vec::new();
        let mut partition_qualified_names = Vec::new();
        let mut num_buckets_list = Vec::new();
        let mut partition_comments = Vec::new();

        for database in &databases {
            let tables = admin.list_tables(database).await.map_err(fluss_err)?;
            for table in tables {
                let path = TablePath::new(database.clone(), table.clone());
                let info = admin.get_table_info(&path).await.map_err(fluss_err)?;
                let num_buckets = info.get_num_buckets() as i32;

                if info.is_partitioned() {
                    // Partitioned table: call list_partition_infos to get real partitions
                    let partitions = admin.list_partition_infos(&path).await.map_err(fluss_err)?;

                    for partition in partitions {
                        table_schemas.push(database.clone());
                        table_names.push(table.clone());
                        partition_ids.push(partition.get_partition_id() as i32);
                        // Partition name: value1$value2$...
                        partition_names.push(partition.get_partition_name());
                        // Qualified name: key1=value1/key2=value2
                        partition_qualified_names.push(
                            partition
                                .get_resolved_partition_spec()
                                .get_partition_qualified_name(),
                        );
                        num_buckets_list.push(num_buckets);
                        partition_comments.push("".to_string());
                    }
                } else {
                    // Non-partitioned table: show as single default partition (partition_id = -1)
                    table_schemas.push(database.clone());
                    table_names.push(table.clone());
                    partition_ids.push(-1i32); // -1 indicates non-partitioned table
                    partition_names.push("__non_partitioned__".to_string());
                    partition_qualified_names.push("".to_string());
                    num_buckets_list.push(num_buckets);
                    partition_comments.push("Non-partitioned table".to_string());
                }
            }
        }

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("partition_id", DataType::Int32, false),
            Field::new("partition_name", DataType::Utf8, false),
            Field::new("partition_qualified_name", DataType::Utf8, false),
            Field::new("num_buckets", DataType::Int32, false),
            Field::new("partition_comment", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(table_schemas)),
                Arc::new(StringArray::from(table_names)),
                Arc::new(Int32Array::from(partition_ids)),
                Arc::new(StringArray::from(partition_names)),
                Arc::new(StringArray::from(partition_qualified_names)),
                Arc::new(Int32Array::from(num_buckets_list)),
                Arc::new(StringArray::from(partition_comments)),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let mem_table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Some(Arc::new(mem_table)))
    }

    /// Build buckets virtual table - For SHOW BUCKETS command
    ///
    /// Uses Fluss Admin API to get real partition information, generates bucket list for each partition
    async fn build_buckets_table(&self) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let admin = self.conn.get_admin().map_err(fluss_err)?;
        let databases = admin.list_databases().await.map_err(fluss_err)?;

        let mut table_schemas = Vec::new();
        let mut table_names = Vec::new();
        let mut partition_ids = Vec::new();
        let mut bucket_ids = Vec::new();
        let mut bucket_keys = Vec::new();
        let mut row_counts = Vec::<Option<i64>>::new();

        for database in &databases {
            let tables = admin.list_tables(database).await.map_err(fluss_err)?;
            for table in tables {
                let path = TablePath::new(database.clone(), table.clone());
                let info = admin.get_table_info(&path).await.map_err(fluss_err)?;

                let num_buckets = info.get_num_buckets();
                let bucket_key_columns = info.get_bucket_keys().join(", ");

                if info.is_partitioned() {
                    // Partitioned table: get all partitions, list buckets for each partition
                    let partitions = admin.list_partition_infos(&path).await.map_err(fluss_err)?;

                    for partition in partitions {
                        let partition_id = partition.get_partition_id() as i32;
                        for bucket_id in 0..num_buckets {
                            table_schemas.push(database.clone());
                            table_names.push(table.clone());
                            partition_ids.push(partition_id);
                            bucket_ids.push(bucket_id as i32);
                            bucket_keys.push(bucket_key_columns.clone());
                            row_counts.push(None); // TODO: get actual row count from lake snapshot
                        }
                    }
                } else {
                    // Non-partitioned table: use -1 as partition ID
                    let partition_id = -1i32;
                    for bucket_id in 0..num_buckets {
                        table_schemas.push(database.clone());
                        table_names.push(table.clone());
                        partition_ids.push(partition_id);
                        bucket_ids.push(bucket_id as i32);
                        bucket_keys.push(bucket_key_columns.clone());
                        row_counts.push(None);
                    }
                }
            }
        }

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("partition_id", DataType::Int32, false),
            Field::new("bucket_id", DataType::Int32, false),
            Field::new("bucket_key", DataType::Utf8, false),
            Field::new("row_count", DataType::Int64, true),
        ]));

        let row_counts_array: Int64Array = row_counts.into_iter().collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(table_schemas)),
                Arc::new(StringArray::from(table_names)),
                Arc::new(Int32Array::from(partition_ids)),
                Arc::new(Int32Array::from(bucket_ids)),
                Arc::new(StringArray::from(bucket_keys)),
                Arc::new(row_counts_array),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let mem_table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Some(Arc::new(mem_table)))
    }

    /// Build table_options virtual table - For SHOW OPTIONS command
    async fn build_table_options_table(&self) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let admin = self.conn.get_admin().map_err(fluss_err)?;
        let databases = admin.list_databases().await.map_err(fluss_err)?;

        let mut table_schemas = Vec::new();
        let mut table_names = Vec::new();
        let mut option_names = Vec::new();
        let mut option_values = Vec::new();
        let mut option_types = Vec::new();

        for database in &databases {
            let tables = admin.list_tables(database).await.map_err(fluss_err)?;
            for table in tables {
                let path = TablePath::new(database.clone(), table.clone());
                let info = admin.get_table_info(&path).await.map_err(fluss_err)?;

                for (k, v) in info.get_properties() {
                    table_schemas.push(database.clone());
                    table_names.push(table.clone());
                    option_names.push(k.clone());
                    option_values.push(v.clone());
                    option_types.push("property".to_string());
                }

                for (k, v) in info.get_custom_properties() {
                    table_schemas.push(database.clone());
                    table_names.push(table.clone());
                    option_names.push(k.clone());
                    option_values.push(v.clone());
                    option_types.push("custom_property".to_string());
                }
            }
        }

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("option_name", DataType::Utf8, false),
            Field::new("option_value", DataType::Utf8, true),
            Field::new("option_type", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(table_schemas)),
                Arc::new(StringArray::from(table_names)),
                Arc::new(StringArray::from(option_names)),
                Arc::new(StringArray::from(option_values)),
                Arc::new(StringArray::from(option_types)),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let mem_table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Some(Arc::new(mem_table)))
    }

    /// Build table_stats virtual table - Display table statistics
    async fn build_table_stats_table(&self) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let admin = self.conn.get_admin().map_err(fluss_err)?;
        let databases = admin.list_databases().await.map_err(fluss_err)?;

        let mut table_schemas = Vec::new();
        let mut table_names = Vec::new();
        let mut row_counts = Vec::<Option<i64>>::new();
        let mut total_sizes = Vec::<Option<i64>>::new();
        let mut last_modified = Vec::new();

        for database in &databases {
            let tables = admin.list_tables(database).await.map_err(fluss_err)?;
            for table in tables {
                table_schemas.push(database.clone());
                table_names.push(table.clone());
                row_counts.push(None);
                total_sizes.push(None);
                last_modified.push("".to_string());
            }
        }

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("row_count", DataType::Int64, true),
            Field::new("total_size_bytes", DataType::Int64, true),
            Field::new("last_modified", DataType::Utf8, true),
        ]));

        let row_counts_array: Int64Array = row_counts.into_iter().collect();
        let total_sizes_array: Int64Array = total_sizes.into_iter().collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(table_schemas)),
                Arc::new(StringArray::from(table_names)),
                Arc::new(row_counts_array),
                Arc::new(total_sizes_array),
                Arc::new(StringArray::from(last_modified)),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let mem_table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Some(Arc::new(mem_table)))
    }
}

/// Format CREATE TABLE SQL statement
fn format_create_table_sql(table_info: &TableInfo) -> String {
    use std::collections::BTreeMap;

    let table_path = table_info.get_table_path();
    let full_name = format!(
        "{}.{}",
        quote_ident(table_path.database()),
        quote_ident(table_path.table())
    );

    let mut definitions: Vec<String> = table_info
        .get_schema()
        .columns()
        .iter()
        .map(|column| {
            let mut def = format!("  {} {}", quote_ident(column.name()), column.data_type());
            if let Some(comment) = column.comment() {
                def.push_str(&format!(" COMMENT {}", quote_literal(comment)));
            }
            def
        })
        .collect();

    if table_info.has_primary_key() {
        let pk = table_info
            .get_primary_keys()
            .iter()
            .map(|key| quote_ident(key))
            .collect::<Vec<_>>()
            .join(", ");
        definitions.push(format!("  PRIMARY KEY ({pk})"));
    }

    let mut ddl = format!("CREATE TABLE {full_name} (\n{}\n)", definitions.join(",\n"));

    if table_info.is_partitioned() {
        let partitioned_by = table_info
            .get_partition_keys()
            .iter()
            .map(|key| quote_ident(key))
            .collect::<Vec<_>>()
            .join(", ");
        ddl.push_str(&format!("\nPARTITIONED BY ({partitioned_by})"));
    }

    if !table_info.get_bucket_keys().is_empty() {
        let distributed_by = table_info
            .get_bucket_keys()
            .iter()
            .map(|key| quote_ident(key))
            .collect::<Vec<_>>()
            .join(", ");
        ddl.push_str(&format!(
            "\nDISTRIBUTED BY ({distributed_by}) INTO {} BUCKETS",
            table_info.get_num_buckets()
        ));
    }

    if let Some(comment) = table_info.get_comment() {
        ddl.push_str(&format!("\nCOMMENT {}", quote_literal(comment)));
    }

    let mut merged_props = BTreeMap::new();
    for (k, v) in table_info.get_properties() {
        merged_props.insert(k.clone(), v.clone());
    }
    for (k, v) in table_info.get_custom_properties() {
        merged_props.insert(k.clone(), v.clone());
    }
    if !merged_props.is_empty() {
        let props = merged_props
            .iter()
            .map(|(k, v)| format!("  {} = {}", quote_literal(k), quote_literal(v)))
            .collect::<Vec<_>>()
            .join(",\n");
        ddl.push_str(&format!("\nWITH (\n{props}\n)"));
    }

    ddl
}

fn quote_ident(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
