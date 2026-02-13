use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
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
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                match conn.get_admin().await {
                    Ok(admin) => admin.list_databases().await.unwrap_or_default(),
                    Err(_) => vec![],
                }
            })
        })
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
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
