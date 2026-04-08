//! SQL Extension Module - Handles Fluss-specific SQL semantics
//!
//! This module provides:
//! 1. SQL command rewriting and extension
//! 2. Fluss-specific SQL dialect support
//! 3. Virtual table definitions (information_schema extensions)

use datafusion::sql::sqlparser;
use datafusion::sql::sqlparser::ast::Statement;

pub mod dialect;
pub mod rewriter;
pub mod show;

// Internal module - not exposed directly
// information_schema implementation is in catalog::schema

/// SQL processing context
#[derive(Debug, Clone)]
pub struct SqlContext {
    pub current_database: String,
    pub default_catalog: String,
}

impl SqlContext {
    pub fn new(current_database: String, default_catalog: String) -> Self {
        Self {
            current_database,
            default_catalog,
        }
    }
}

/// SQL processing result
#[derive(Debug)]
pub enum ProcessedSql {
    /// SQL that can be executed directly
    Direct(String),
    /// SQL that needs rewriting (e.g., SHOW commands converted to SELECT)
    Rewritten(String),
    /// Special commands (non-SQL, like meta-commands)
    SpecialCommand(String),
}

/// Parse SQL statements
pub fn parse_sql(sql: &str) -> anyhow::Result<Vec<Statement>> {
    let dialect = dialect::fluss_sql_dialect();
    let statements = sqlparser::parser::Parser::parse_sql(&dialect, sql)
        .map_err(|e| anyhow::anyhow!("SQL parse error: {}", e))?;
    Ok(statements)
}
