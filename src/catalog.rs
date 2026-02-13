use std::any::Any;
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use fluss::client::FlussConnection;
use fluss::metadata::TablePath;

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
        vec!["tables".to_string()]
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if !name.eq_ignore_ascii_case("tables") {
            return Ok(None);
        }

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

    fn table_exist(&self, name: &str) -> bool {
        name.eq_ignore_ascii_case("tables")
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
