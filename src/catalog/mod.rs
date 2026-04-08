//! Catalog Module - Fluss and DataFusion catalog layer integration
//!
//! This module provides:
//! - FlussCatalog: Maps Fluss cluster to DataFusion Catalog
//! - FlussSchema: Maps Fluss database to DataFusion Schema
//!
//! Catalog structure:
//! - FlussCluster (FlussCatalog) -> DataFusion Catalog
//!   - FlussDatabase (FlussSchema) -> DataFusion Schema
//!     - FlussTable -> DataFusion TableProvider
//!   - information_schema (extended virtual tables)

mod schema;

pub use schema::{FlussInformationSchema, FlussSchema};

use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use fluss::client::FlussConnection;

use crate::sql::SqlContext;

/// Fluss Catalog - Maps Fluss cluster to DataFusion Catalog
pub struct FlussCatalog {
    conn: Arc<FlussConnection>,
    default_db: String,
    sql_ctx: SqlContext,
}

impl std::fmt::Debug for FlussCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlussCatalog")
            .field("default_db", &self.default_db)
            .field("catalog", &self.sql_ctx.default_catalog)
            .finish()
    }
}

impl FlussCatalog {
    pub fn new(conn: Arc<FlussConnection>, default_db: String) -> Self {
        let sql_ctx = SqlContext::new(default_db.clone(), "fluss".to_string());
        Self {
            conn,
            default_db,
            sql_ctx,
        }
    }

    /// Get SQL context
    pub fn sql_context(&self) -> &SqlContext {
        &self.sql_ctx
    }

    /// Update current database
    pub fn set_current_database(&mut self, db: String) {
        self.default_db = db.clone();
        self.sql_ctx = SqlContext::new(db, self.sql_ctx.default_catalog.clone());
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
                match conn.get_admin() {
                    Ok(admin) => admin.list_databases().await.unwrap_or_default(),
                    Err(_) => vec![],
                }
            })
        });

        // Ensure information_schema is always available
        if !names
            .iter()
            .any(|name| name.eq_ignore_ascii_case("information_schema"))
        {
            names.push("information_schema".to_string());
        }

        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name.eq_ignore_ascii_case("information_schema") {
            return Some(Arc::new(FlussInformationSchema::new(
                self.conn.clone(),
                self.sql_ctx.clone(),
            )));
        }
        Some(Arc::new(FlussSchema::new(
            self.conn.clone(),
            name.to_string(),
        )))
    }
}
