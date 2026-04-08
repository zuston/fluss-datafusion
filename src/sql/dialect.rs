//! Fluss SQL Dialect Definition
//!
//! Extends DataFusion's SQL parser to support Fluss-specific syntax

use datafusion::sql::sqlparser::dialect::Dialect;

/// Fluss SQL Dialect
#[derive(Debug, Clone, Copy)]
pub struct FlussDialect {
    /// Whether to support double-quoted identifiers
    pub double_quoted_identifiers: bool,
    /// Whether to support backtick-quoted identifiers
    pub back_quoted_identifiers: bool,
}

impl Default for FlussDialect {
    fn default() -> Self {
        Self {
            double_quoted_identifiers: true,
            back_quoted_identifiers: true,
        }
    }
}

impl Dialect for FlussDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        // Support standard identifier start characters and Unicode
        ch.is_alphabetic() || ch == '_' || ch == '@'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        // Support standard identifier part characters
        ch.is_alphabetic() || ch.is_ascii_digit() || ch == '_' || ch == '@' || ch == '$'
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        // Support double quotes and backticks as delimited identifiers
        (self.double_quoted_identifiers && ch == '"') || (self.back_quoted_identifiers && ch == '`')
    }

    fn identifier_quote_style(&self, _ch: &str) -> Option<char> {
        // Identifier quote style - use backticks
        Some('`')
    }
}

/// Create Fluss SQL dialect
pub fn fluss_sql_dialect() -> FlussDialect {
    FlussDialect::default()
}

/// Check if SQL is a Fluss-specific command
pub fn is_fluss_special_command(sql: &str) -> bool {
    let trimmed = sql.trim().to_ascii_lowercase();
    let starts_with = [
        "show partitions",
        "show buckets",
        "show options",
        "show table options",
        "describe table",
        "describe partitions",
    ];

    for prefix in &starts_with {
        if trimmed.starts_with(prefix) {
            return true;
        }
    }
    false
}

/// Extract table name from special commands
/// e.g.: "SHOW PARTITIONS my_table" -> "my_table"
pub fn extract_table_name_from_show(sql: &str) -> Option<String> {
    let trimmed = sql.trim().to_ascii_lowercase();

    // Match "SHOW PARTITIONS <table>" pattern
    let patterns = [
        "show partitions",
        "show buckets",
        "show options",
        "show table options",
        "describe table",
        "describe partitions",
    ];

    for pattern in &patterns {
        if trimmed.starts_with(pattern) {
            let table_part = sql.trim()[pattern.len()..].trim();
            return Some(table_part.trim_end_matches(';').trim().to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_fluss_special_command() {
        assert!(is_fluss_special_command("SHOW PARTITIONS my_table"));
        assert!(is_fluss_special_command("show partitions my_table"));
        assert!(is_fluss_special_command("SHOW BUCKETS my_table"));
        assert!(is_fluss_special_command("DESCRIBE TABLE my_table"));
        assert!(!is_fluss_special_command("SELECT * FROM my_table"));
        assert!(!is_fluss_special_command("SHOW TABLES"));
    }

    #[test]
    fn test_extract_table_name() {
        assert_eq!(
            extract_table_name_from_show("SHOW PARTITIONS my_table"),
            Some("my_table".to_string())
        );
        assert_eq!(
            extract_table_name_from_show("show partitions `my-db`.`my-table`;"),
            Some("`my-db`.`my-table`".to_string())
        );
        assert_eq!(
            extract_table_name_from_show("DESCRIBE TABLE my_table"),
            Some("my_table".to_string())
        );
    }
}
