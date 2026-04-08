//! SHOW command support module
//!
//! Provides Fluss-specific SHOW command implementations, rewriting SQL to information_schema queries
//!
//! Supported commands:
//! - SHOW TABLES [FROM|IN db_name]
//! - SHOW CREATE TABLE table_name
//! - SHOW PARTITIONS table_name
//! - SHOW BUCKETS table_name
//! - SHOW [TABLE] OPTIONS table_name
//! - SHOW DATABASES | SHOW SCHEMAS
//!
//! Note: Actual execution converts to information_schema queries via SQL rewriter

use crate::sql::SqlContext;

/// SHOW command types
#[derive(Debug, Clone, PartialEq)]
pub enum ShowCommand {
    /// SHOW TABLES [FROM db_name]
    Tables(Option<String>),
    /// SHOW CREATE TABLE [db.]table_name
    CreateTable(String, String),
    /// SHOW PARTITIONS [db.]table_name
    Partitions(String, String),
    /// SHOW BUCKETS [db.]table_name
    Buckets(String, String),
    /// SHOW [TABLE] OPTIONS [db.]table_name
    TableOptions(String, String),
    /// SHOW DATABASES | SHOW SCHEMAS
    Databases,
}

impl ShowCommand {
    /// Convert SHOW command to DataFusion SQL
    pub fn to_sql(&self, ctx: &SqlContext) -> String {
        let escaped = |s: &str| s.replace('\'', "''");

        match self {
            ShowCommand::Tables(None) => {
                format!(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' ORDER BY table_name",
                    escaped(&ctx.current_database)
                )
            }
            ShowCommand::Tables(Some(db)) => {
                format!(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' ORDER BY table_name",
                    escaped(db)
                )
            }
            ShowCommand::CreateTable(db, table) => {
                format!(
                    "SELECT create_table FROM information_schema.table_ddl WHERE table_schema = '{}' AND table_name = '{}'",
                    escaped(db), escaped(table)
                )
            }
            ShowCommand::Partitions(db, table) => {
                format!(
                    "SELECT partition_id, partition_name, partition_qualified_name, num_buckets FROM information_schema.partitions WHERE table_schema = '{}' AND table_name = '{}' AND partition_id >= 0 ORDER BY partition_id",
                    escaped(db), escaped(table)
                )
            }
            ShowCommand::Buckets(db, table) => {
                format!(
                    "SELECT bucket_id, bucket_key, partition_id FROM information_schema.buckets WHERE table_schema = '{}' AND table_name = '{}' ORDER BY partition_id, bucket_id",
                    escaped(db), escaped(table)
                )
            }
            ShowCommand::TableOptions(db, table) => {
                format!(
                    "SELECT option_name, option_value, option_type FROM information_schema.table_options WHERE table_schema = '{}' AND table_name = '{}' ORDER BY option_name",
                    escaped(db), escaped(table)
                )
            }
            ShowCommand::Databases => {
                "SELECT schema_name as database_name FROM information_schema.schemata ORDER BY schema_name".to_string()
            }
        }
    }

    /// Get command description
    pub fn description(&self) -> String {
        match self {
            ShowCommand::Tables(None) => "List tables in current database".to_string(),
            ShowCommand::Tables(Some(db)) => format!("List tables in database {}", db),
            ShowCommand::CreateTable(_, table) => {
                format!("Show CREATE TABLE statement for {}", table)
            }
            ShowCommand::Partitions(_, table) => {
                format!("Show partition information for table {}", table)
            }
            ShowCommand::Buckets(_, table) => {
                format!("Show bucket information for table {}", table)
            }
            ShowCommand::TableOptions(_, table) => format!("Show table options for {}", table),
            ShowCommand::Databases => "List all databases".to_string(),
        }
    }
}

/// Parse SHOW/DESCRIBE commands
///
/// Returns Some(ShowCommand) if parsed successfully, otherwise returns None
pub fn parse_show_command(sql: &str, current_db: &str) -> Option<ShowCommand> {
    use crate::sql::rewriter::{
        parse_describe_table, parse_show_buckets, parse_show_create_table, parse_show_partitions,
        parse_show_table_options, parse_show_tables,
    };

    let trimmed = sql.trim().trim_end_matches(';').trim();

    // SHOW DATABASES | SHOW SCHEMAS
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    let tokens_lower: Vec<String> = tokens.iter().map(|t| t.to_ascii_lowercase()).collect();

    if tokens_lower.len() == 2
        && tokens_lower[0] == "show"
        && (tokens_lower[1] == "databases" || tokens_lower[1] == "schemas")
    {
        return Some(ShowCommand::Databases);
    }

    // SHOW TABLES
    if let Some(db) = parse_show_tables(trimmed) {
        return Some(ShowCommand::Tables(db));
    }

    // SHOW CREATE TABLE
    if let Some((db, table)) = parse_show_create_table(trimmed, current_db) {
        return Some(ShowCommand::CreateTable(db, table));
    }

    // SHOW PARTITIONS
    if let Some((db, table)) = parse_show_partitions(trimmed, current_db) {
        return Some(ShowCommand::Partitions(db, table));
    }

    // SHOW BUCKETS
    if let Some((db, table)) = parse_show_buckets(trimmed, current_db) {
        return Some(ShowCommand::Buckets(db, table));
    }

    // SHOW [TABLE] OPTIONS
    if let Some((db, table)) = parse_show_table_options(trimmed, current_db) {
        return Some(ShowCommand::TableOptions(db, table));
    }

    // DESCRIBE [TABLE] (not a ShowCommand, but parsed for completeness)
    if let Some((db, table)) = parse_describe_table(trimmed, current_db) {
        // DESCRIBE returns DESCRIBE type info, create a special command here
        return Some(ShowCommand::TableOptions(db, table));
    }

    None
}

/// Get help information for all supported SHOW commands
pub fn show_commands_help() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "SHOW TABLES [FROM db_name]",
            "List all tables in the specified database (or current database)",
        ),
        (
            "SHOW CREATE TABLE table_name",
            "Show the CREATE TABLE statement for the specified table",
        ),
        (
            "SHOW PARTITIONS table_name",
            "Show partition information for the specified table",
        ),
        (
            "SHOW BUCKETS table_name",
            "Show bucket information for the specified table",
        ),
        (
            "SHOW [TABLE] OPTIONS table_name",
            "Show table options/properties for the specified table",
        ),
        ("SHOW DATABASES", "List all databases"),
        ("SHOW SCHEMAS", "Alias for SHOW DATABASES"),
        (
            "DESCRIBE table_name",
            "Show column information for the specified table",
        ),
        ("DESC table_name", "Alias for DESCRIBE"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> SqlContext {
        SqlContext::new("test_db".to_string(), "test_catalog".to_string())
    }

    #[test]
    fn test_parse_show_tables() {
        assert_eq!(
            parse_show_command("SHOW TABLES", "db"),
            Some(ShowCommand::Tables(None))
        );
        assert_eq!(
            parse_show_command("SHOW TABLES FROM mydb", "db"),
            Some(ShowCommand::Tables(Some("mydb".to_string())))
        );
        assert_eq!(
            parse_show_command("SHOW TABLES IN mydb", "db"),
            Some(ShowCommand::Tables(Some("mydb".to_string())))
        );
    }

    #[test]
    fn test_parse_show_databases() {
        assert_eq!(
            parse_show_command("SHOW DATABASES", "db"),
            Some(ShowCommand::Databases)
        );
        assert_eq!(
            parse_show_command("SHOW SCHEMAS", "db"),
            Some(ShowCommand::Databases)
        );
    }

    #[test]
    fn test_parse_show_create_table() {
        let cmd = parse_show_command("SHOW CREATE TABLE my_table", "db").unwrap();
        match cmd {
            ShowCommand::CreateTable(db, table) => {
                assert_eq!(db, "db");
                assert_eq!(table, "my_table");
            }
            _ => panic!("Expected CreateTable"),
        }

        let cmd = parse_show_command("SHOW CREATE TABLE mydb.my_table", "db").unwrap();
        match cmd {
            ShowCommand::CreateTable(db, table) => {
                assert_eq!(db, "mydb");
                assert_eq!(table, "my_table");
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_show_partitions() {
        let cmd = parse_show_command("SHOW PARTITIONS my_table", "db").unwrap();
        match cmd {
            ShowCommand::Partitions(db, table) => {
                assert_eq!(db, "db");
                assert_eq!(table, "my_table");
            }
            _ => panic!("Expected Partitions"),
        }
    }

    #[test]
    fn test_command_to_sql() {
        let ctx = ctx();

        assert!(ShowCommand::Tables(None)
            .to_sql(&ctx)
            .contains("information_schema.tables"));

        let sql = ShowCommand::Partitions("db".to_string(), "table".to_string()).to_sql(&ctx);
        assert!(sql.contains("information_schema.partitions"));
        assert!(sql.contains("partition_qualified_name"));
        assert!(sql.contains("partition_id >= 0")); // Only show real partitions
    }

    #[test]
    fn test_not_show_command() {
        assert!(parse_show_command("SELECT * FROM t", "db").is_none());
        assert!(parse_show_command("INSERT INTO t VALUES (1)", "db").is_none());
    }
}
