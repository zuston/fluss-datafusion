use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::Constraint;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use fluss::client::FlussConnection;
use fluss::metadata::{
    DataType as FlussDataType, DataTypes, Schema as FlussTableSchema, TableDescriptor, TableInfo,
    TablePath,
};

use crate::error::fluss_err;
use crate::provider::FlussTableProvider;

// ---------------------------------------------------------------------------
// FlussCatalog – maps to a Fluss cluster, lists databases as schemas.
// ---------------------------------------------------------------------------

pub struct FlussCatalog {
    conn: Arc<FlussConnection>,
    default_db: String,
}

impl std::fmt::Debug for FlussCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlussCatalog")
            .field("default_db", &self.default_db)
            .finish()
    }
}

impl FlussCatalog {
    pub fn new(conn: Arc<FlussConnection>, default_db: String) -> Self {
        Self { conn, default_db }
    }
}

impl CatalogProvider for FlussCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        // Sync context – use block_in_place to query Fluss for databases.
        let conn = self.conn.clone();
        let mut names = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                match conn.get_admin().await {
                    Ok(admin) => admin.list_databases().await.unwrap_or_default(),
                    Err(_) => vec![],
                }
            })
        });
        if !names.iter().any(|name| name == "information_schema") {
            names.push("information_schema".to_string());
        }
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name.eq_ignore_ascii_case("information_schema") {
            return Some(Arc::new(FlussInformationSchema::new(self.conn.clone())));
        }
        Some(Arc::new(FlussSchema::new(
            self.conn.clone(),
            name.to_string(),
        )))
    }
}

// ---------------------------------------------------------------------------
// FlussSchema – maps to a Fluss database, lists / resolves tables.
// ---------------------------------------------------------------------------

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

struct FlussInformationSchema {
    conn: Arc<FlussConnection>,
}

impl FlussInformationSchema {
    fn new(conn: Arc<FlussConnection>) -> Self {
        Self { conn }
    }
}

impl std::fmt::Debug for FlussInformationSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlussInformationSchema").finish()
    }
}

#[async_trait]
impl SchemaProvider for FlussInformationSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec!["tables".to_string(), "table_ddl".to_string()]
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if name.eq_ignore_ascii_case("tables") {
            let admin = self.conn.get_admin().await.map_err(fluss_err)?;
            let databases = admin.list_databases().await.map_err(fluss_err)?;

            let mut table_schemas = Vec::new();
            let mut table_names = Vec::new();
            let mut table_types = Vec::new();

            for database in databases {
                let tables = admin.list_tables(&database).await.map_err(fluss_err)?;
                for table in tables {
                    table_schemas.push(database.clone());
                    table_names.push(table);
                    table_types.push("BASE TABLE".to_string());
                }
            }

            let schema = Arc::new(Schema::new(vec![
                Field::new("table_schema", ArrowDataType::Utf8, false),
                Field::new("table_name", ArrowDataType::Utf8, false),
                Field::new("table_type", ArrowDataType::Utf8, false),
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

            return Ok(Some(Arc::new(mem_table)));
        }

        if name.eq_ignore_ascii_case("table_ddl") {
            let admin = self.conn.get_admin().await.map_err(fluss_err)?;
            let databases = admin.list_databases().await.map_err(fluss_err)?;

            let mut table_schemas = Vec::new();
            let mut table_names = Vec::new();
            let mut create_table_sql = Vec::new();

            for database in databases {
                let tables = admin.list_tables(&database).await.map_err(fluss_err)?;
                for table in tables {
                    let path = TablePath::new(database.clone(), table.clone());
                    let info = admin.get_table_info(&path).await.map_err(fluss_err)?;

                    table_schemas.push(database.clone());
                    table_names.push(table);
                    create_table_sql.push(format_create_table_sql(&info));
                }
            }

            let schema = Arc::new(Schema::new(vec![
                Field::new("table_schema", ArrowDataType::Utf8, false),
                Field::new("table_name", ArrowDataType::Utf8, false),
                Field::new("create_table", ArrowDataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(table_schemas)),
                    Arc::new(StringArray::from(table_names)),
                    Arc::new(StringArray::from(create_table_sql)),
                ],
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            let mem_table = MemTable::try_new(schema, vec![vec![batch]])?;

            return Ok(Some(Arc::new(mem_table)));
        }

        Ok(None)
    }

    fn table_exist(&self, name: &str) -> bool {
        name.eq_ignore_ascii_case("tables") || name.eq_ignore_ascii_case("table_ddl")
    }
}

fn format_create_table_sql(table_info: &TableInfo) -> String {
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

impl FlussSchema {
    pub fn new(conn: Arc<FlussConnection>, database: String) -> Self {
        Self { conn, database }
    }
}

fn datafusion_table_to_fluss_descriptor(table: &Arc<dyn TableProvider>) -> Result<TableDescriptor> {
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

fn arrow_to_fluss_type(data_type: &ArrowDataType) -> Result<FlussDataType> {
    match data_type {
        ArrowDataType::Boolean => Ok(DataTypes::boolean()),
        ArrowDataType::Int8 | ArrowDataType::UInt8 => Ok(DataTypes::tinyint()),
        ArrowDataType::Int16 | ArrowDataType::UInt16 => Ok(DataTypes::smallint()),
        ArrowDataType::Int32 | ArrowDataType::UInt32 => Ok(DataTypes::int()),
        ArrowDataType::Int64 | ArrowDataType::UInt64 => Ok(DataTypes::bigint()),
        ArrowDataType::Float32 => Ok(DataTypes::float()),
        ArrowDataType::Float64 => Ok(DataTypes::double()),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
            Ok(DataTypes::string())
        }
        ArrowDataType::Binary | ArrowDataType::LargeBinary | ArrowDataType::FixedSizeBinary(_) => {
            Ok(DataTypes::bytes())
        }
        ArrowDataType::Date32 | ArrowDataType::Date64 => Ok(DataTypes::date()),
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => Ok(DataTypes::time()),
        ArrowDataType::Timestamp(_, _) => Ok(DataTypes::timestamp()),
        ArrowDataType::Decimal128(precision, scale) => {
            let scale = u32::try_from(*scale).map_err(|_| {
                DataFusionError::Execution(format!(
                    "negative decimal scale is unsupported: {scale}"
                ))
            })?;
            Ok(DataTypes::decimal(u32::from(*precision), scale))
        }
        ArrowDataType::Decimal256(precision, scale) => {
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
                match conn.get_admin().await {
                    Ok(admin) => admin.list_tables(&db).await.unwrap_or_default(),
                    Err(_) => vec![],
                }
            })
        })
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let path = TablePath::new(self.database.clone(), name.to_string());
        let admin = self.conn.get_admin().await.map_err(fluss_err)?;
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
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let descriptor = datafusion_table_to_fluss_descriptor(&table)?;
        let conn = self.conn.clone();
        let database = self.database.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let admin = conn.get_admin().await.map_err(fluss_err)?;
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
                match conn.get_admin().await {
                    Ok(admin) => admin.table_exists(&path).await.unwrap_or(false),
                    Err(_) => false,
                }
            })
        })
    }
}
