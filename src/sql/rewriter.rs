//! SQL Rewriting Module
//!
//! Rewrites Fluss-specific SQL commands to standard DataFusion SQL
//! Examples:
//! - SHOW PARTITIONS table_name -> SELECT * FROM information_schema.partitions WHERE ...
//! - SHOW CREATE TABLE table_name -> SELECT * FROM information_schema.table_ddl WHERE ...

use crate::sql::SqlContext;

/// Rewrite SQL statements to DataFusion executable SQL
///
/// # Supported commands
/// - SHOW TABLES [FROM db] -> SELECT ... FROM information_schema.tables
/// - SHOW CREATE TABLE name -> SELECT ... FROM information_schema.table_ddl
/// - SHOW PARTITIONS name -> SELECT ... FROM information_schema.partitions
/// - SHOW BUCKETS name -> SELECT ... FROM information_schema.buckets
/// - SHOW OPTIONS name -> SELECT ... FROM information_schema.table_options
/// - DESCRIBE TABLE name -> SELECT ... FROM information_schema.columns
pub fn rewrite_sql(sql: &str, ctx: &SqlContext) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();

    // SHOW TABLES
    if let Some(db) = parse_show_tables(trimmed) {
        let database = db.unwrap_or_else(|| ctx.current_database.clone());
        let escaped_db = escape_sql_string(&database);
        return Some(format!(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = '{escaped_db}' ORDER BY table_name"
        ));
    }

    // SHOW CREATE TABLE
    if let Some((db, table)) = parse_show_create_table(trimmed, &ctx.current_database) {
        let escaped_db = escape_sql_string(&db);
        let escaped_table = escape_sql_string(&table);
        return Some(format!(
            "SELECT create_table FROM information_schema.table_ddl WHERE table_schema = '{escaped_db}' AND table_name = '{escaped_table}'"
        ));
    }

    // SHOW PARTITIONS table_name
    if let Some((db, table)) = parse_show_partitions(trimmed, &ctx.current_database) {
        let escaped_db = escape_sql_string(&db);
        let escaped_table = escape_sql_string(&table);
        return Some(format!(
            "SELECT partition_id, partition_name, partition_qualified_name, num_buckets FROM information_schema.partitions WHERE table_schema = '{escaped_db}' AND table_name = '{escaped_table}' AND partition_id >= 0 ORDER BY partition_id"
        ));
    }

    // SHOW BUCKETS table_name
    if let Some((db, table)) = parse_show_buckets(trimmed, &ctx.current_database) {
        let escaped_db = escape_sql_string(&db);
        let escaped_table = escape_sql_string(&table);
        return Some(format!(
            "SELECT bucket_id, bucket_key, partition_id, stats FROM information_schema.buckets WHERE table_schema = '{escaped_db}' AND table_name = '{escaped_table}' ORDER BY partition_id, bucket_id"
        ));
    }

    // SHOW OPTIONS table_name or SHOW TABLE OPTIONS table_name
    if let Some((db, table)) = parse_show_table_options(trimmed, &ctx.current_database) {
        let escaped_db = escape_sql_string(&db);
        let escaped_table = escape_sql_string(&table);
        return Some(format!(
            "SELECT option_name, option_value, option_type FROM information_schema.table_options WHERE table_schema = '{escaped_db}' AND table_name = '{escaped_table}' ORDER BY option_name"
        ));
    }

    // DESCRIBE table_name or DESCRIBE TABLE table_name
    if let Some((db, table)) = parse_describe_table(trimmed, &ctx.current_database) {
        let escaped_db = escape_sql_string(&db);
        let escaped_table = escape_sql_string(&table);
        return Some(format!(
            "SELECT column_name, data_type, is_nullable, column_default, column_comment, is_primary_key FROM information_schema.columns WHERE table_schema = '{escaped_db}' AND table_name = '{escaped_table}' ORDER BY ordinal_position"
        ));
    }

    None
}

/// Parse SHOW TABLES [FROM|IN db_name] statements
///
/// Returns:
/// - Some(Some(db)) - database name specified
/// - Some(None) - use current database
/// - None - not a SHOW TABLES statement
pub fn parse_show_tables(sql: &str) -> Option<Option<String>> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    if tokens.len() < 2 {
        return None;
    }

    let tokens_lower: Vec<String> = tokens.iter().map(|t| t.to_ascii_lowercase()).collect();

    // SHOW TABLES
    if tokens_lower.len() == 2 && tokens_lower[0] == "show" && tokens_lower[1] == "tables" {
        return Some(None);
    }

    // SHOW TABLES FROM db or SHOW TABLES IN db
    if tokens_lower.len() == 4
        && tokens_lower[0] == "show"
        && tokens_lower[1] == "tables"
        && (tokens_lower[2] == "from" || tokens_lower[2] == "in")
    {
        return Some(Some(unquote_identifier(tokens[3]).to_string()));
    }

    None
}

/// Parse SHOW CREATE TABLE [db.]table_name statements
pub fn parse_show_create_table(sql: &str, current_db: &str) -> Option<(String, String)> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    if tokens.len() != 4 {
        return None;
    }

    let tokens_lower: Vec<String> = tokens.iter().map(|t| t.to_ascii_lowercase()).collect();

    if tokens_lower[0] != "show" || tokens_lower[1] != "create" || tokens_lower[2] != "table" {
        return None;
    }

    Some(parse_table_name(tokens[3], current_db))
}

/// Parse SHOW PARTITIONS [db.]table_name statements
pub fn parse_show_partitions(sql: &str, current_db: &str) -> Option<(String, String)> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    if tokens.len() != 3 {
        return None;
    }

    let tokens_lower: Vec<String> = tokens.iter().map(|t| t.to_ascii_lowercase()).collect();

    if tokens_lower[0] != "show" || tokens_lower[1] != "partitions" {
        return None;
    }

    Some(parse_table_name(tokens[2], current_db))
}

/// Parse SHOW BUCKETS [db.]table_name statements
pub fn parse_show_buckets(sql: &str, current_db: &str) -> Option<(String, String)> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    if tokens.len() != 3 {
        return None;
    }

    let tokens_lower: Vec<String> = tokens.iter().map(|t| t.to_ascii_lowercase()).collect();

    if tokens_lower[0] != "show" || tokens_lower[1] != "buckets" {
        return None;
    }

    Some(parse_table_name(tokens[2], current_db))
}

/// Parse SHOW [TABLE] OPTIONS [db.]table_name statements
pub fn parse_show_table_options(sql: &str, current_db: &str) -> Option<(String, String)> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    // Supports two forms: SHOW OPTIONS table_name or SHOW TABLE OPTIONS table_name
    let (table_idx, _) = if tokens.len() == 3 {
        // SHOW OPTIONS table_name
        let lower: Vec<String> = tokens.iter().map(|t| t.to_ascii_lowercase()).collect();
        if lower[0] != "show" || lower[1] != "options" {
            return None;
        }
        (2, 3)
    } else if tokens.len() == 4 {
        // SHOW TABLE OPTIONS table_name
        let lower: Vec<String> = tokens.iter().map(|t| t.to_ascii_lowercase()).collect();
        if lower[0] != "show" || lower[1] != "table" || lower[2] != "options" {
            return None;
        }
        (3, 4)
    } else {
        return None;
    };

    Some(parse_table_name(tokens[table_idx], current_db))
}

/// Parse DESCRIBE [TABLE] [db.]table_name statements
pub fn parse_describe_table(sql: &str, current_db: &str) -> Option<(String, String)> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    let lower: Vec<String> = tokens.iter().map(|t| t.to_ascii_lowercase()).collect();

    let table_idx = if tokens.len() == 2 {
        // DESCRIBE table_name or DESC table_name
        if lower[0] != "describe" && lower[0] != "desc" {
            return None;
        }
        1
    } else if tokens.len() == 3 {
        // DESCRIBE TABLE table_name
        if lower[0] == "describe" && lower[1] == "table" {
            2
        } else {
            return None;
        }
    } else {
        return None;
    };

    Some(parse_table_name(tokens[table_idx], current_db))
}

/// Parse table name, handling database.table format
/// Supports quoted identifiers like `my-db`.`my-table`
fn parse_table_name(name: &str, current_db: &str) -> (String, String) {
    let name_trimmed = name.trim();

    // Find last '.', but handle quote-wrapped cases
    // e.g.: `my-db`.`my-table` or db.`my-table` or `my-db`.table
    if let Some(dot_pos) = find_table_name_separator(name_trimmed) {
        let db_part = &name_trimmed[..dot_pos];
        let table_part = &name_trimmed[dot_pos + 1..];
        return (
            unquote_identifier(db_part).to_string(),
            unquote_identifier(table_part).to_string(),
        );
    }

    // Only table name, no database name
    (
        current_db.to_string(),
        unquote_identifier(name_trimmed).to_string(),
    )
}

/// Find table name separator position, handling quoted cases
/// e.g.: `my-db`.`my-table` -> find '.' outside backticks
fn find_table_name_separator(name: &str) -> Option<usize> {
    let mut in_backticks = false;
    let mut in_double_quotes = false;

    for (i, ch) in name.char_indices().rev() {
        match ch {
            '`' if !in_double_quotes => in_backticks = !in_backticks,
            '"' if !in_backticks => in_double_quotes = !in_double_quotes,
            '.' if !in_backticks && !in_double_quotes => return Some(i),
            _ => {}
        }
    }
    None
}

/// Remove quotes from identifier (backticks or double quotes)
fn unquote_identifier(input: &str) -> &str {
    input
        .strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
        .or_else(|| input.strip_prefix('"').and_then(|s| s.strip_suffix('"')))
        .unwrap_or(input)
}

/// Escape single quotes in SQL strings
fn escape_sql_string(input: &str) -> String {
    input.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> SqlContext {
        SqlContext::new("fluss".to_string(), "fluss_catalog".to_string())
    }

    #[test]
    fn test_rewrite_show_tables() {
        let ctx = ctx();

        // SHOW TABLES
        let result = rewrite_sql("SHOW TABLES", &ctx).unwrap();
        assert!(result.contains("information_schema.tables"));
        assert!(result.contains("fluss"));

        // SHOW TABLES FROM mydb
        let result = rewrite_sql("SHOW TABLES FROM mydb", &ctx).unwrap();
        assert!(result.contains("mydb"));

        // SHOW TABLES IN mydb
        let result = rewrite_sql("SHOW TABLES IN mydb", &ctx).unwrap();
        assert!(result.contains("mydb"));
    }

    #[test]
    fn test_rewrite_show_create_table() {
        let ctx = ctx();

        let result = rewrite_sql("SHOW CREATE TABLE my_table", &ctx).unwrap();
        assert!(result.contains("information_schema.table_ddl"));
        assert!(result.contains("fluss"));
        assert!(result.contains("my_table"));

        // With database name
        let result = rewrite_sql("SHOW CREATE TABLE mydb.my_table", &ctx).unwrap();
        assert!(result.contains("mydb"));
        assert!(result.contains("my_table"));

        // With quotes
        let result = rewrite_sql("SHOW CREATE TABLE `my-db`.`my-table`", &ctx).unwrap();
        assert!(result.contains("my-db"));
        assert!(result.contains("my-table"));
    }

    #[test]
    fn test_rewrite_show_partitions() {
        let ctx = ctx();

        let result = rewrite_sql("SHOW PARTITIONS my_table", &ctx).unwrap();
        assert!(result.contains("information_schema.partitions"));
        assert!(result.contains("fluss"));
        assert!(result.contains("my_table"));
        assert!(result.contains("partition_id"));
        assert!(result.contains("partition_qualified_name"));
        assert!(result.contains("partition_id >= 0")); // Filter out default partitions for non-partitioned tables
    }

    #[test]
    fn test_rewrite_show_buckets() {
        let ctx = ctx();

        let result = rewrite_sql("SHOW BUCKETS my_table", &ctx).unwrap();
        assert!(result.contains("information_schema.buckets"));
        assert!(result.contains("bucket_id"));
    }

    #[test]
    fn test_rewrite_show_options() {
        let ctx = ctx();

        let result = rewrite_sql("SHOW OPTIONS my_table", &ctx).unwrap();
        assert!(result.contains("information_schema.table_options"));
        assert!(result.contains("option_name"));
    }

    #[test]
    fn test_rewrite_show_table_options() {
        let ctx = ctx();

        let result = rewrite_sql("SHOW TABLE OPTIONS my_table", &ctx).unwrap();
        assert!(result.contains("information_schema.table_options"));
    }

    #[test]
    fn test_rewrite_describe() {
        let ctx = ctx();

        let result = rewrite_sql("DESCRIBE my_table", &ctx).unwrap();
        assert!(result.contains("information_schema.columns"));
        assert!(result.contains("column_name"));

        let result = rewrite_sql("DESCRIBE TABLE my_table", &ctx).unwrap();
        assert!(result.contains("information_schema.columns"));

        let result = rewrite_sql("DESC my_table", &ctx).unwrap();
        assert!(result.contains("information_schema.columns"));
    }

    #[test]
    fn test_no_rewrite_standard_sql() {
        let ctx = ctx();

        assert!(rewrite_sql("SELECT * FROM my_table", &ctx).is_none());
        assert!(rewrite_sql("INSERT INTO my_table VALUES (1)", &ctx).is_none());
        assert!(rewrite_sql("CREATE TABLE my_table (id INT)", &ctx).is_none());
    }

    #[test]
    fn test_parse_table_name() {
        assert_eq!(
            parse_table_name("my_table", "current_db"),
            ("current_db".to_string(), "my_table".to_string())
        );
        assert_eq!(
            parse_table_name("mydb.my_table", "current_db"),
            ("mydb".to_string(), "my_table".to_string())
        );
        assert_eq!(
            parse_table_name("`my-db`.`my-table`", "current_db"),
            ("my-db".to_string(), "my-table".to_string())
        );
        assert_eq!(
            parse_table_name("`my-db`.my_table", "current_db"),
            ("my-db".to_string(), "my_table".to_string())
        );
        assert_eq!(
            parse_table_name("my_db.`my-table`", "current_db"),
            ("my_db".to_string(), "my-table".to_string())
        );
    }

    #[test]
    fn test_escape_sql_string() {
        assert_eq!(escape_sql_string("hello"), "hello");
        assert_eq!(escape_sql_string("it's"), "it''s");
        assert_eq!(escape_sql_string("a'b'c"), "a''b''c");
    }
}
