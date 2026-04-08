//! CLI Interactive SQL Session
//!
//! Provides interactive REPL, supporting:
//! - Standard SQL statement execution
//! - Fluss-specific SHOW/DESCRIBE commands
//! - Meta-commands (\dt, \q, \?)

use std::sync::Arc;

use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use fluss::client::FlussConnection;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use crate::sql::{rewriter::rewrite_sql, SqlContext};

/// Interactive SQL session
pub struct FlussCliSession {
    ctx: SessionContext,
    conn: Arc<FlussConnection>,
}

impl FlussCliSession {
    pub fn new(ctx: SessionContext, conn: Arc<FlussConnection>) -> Self {
        Self { ctx, conn }
    }

    /// Run interactive REPL
    pub async fn run(&mut self) {
        let mut rl = match DefaultEditor::new() {
            Ok(rl) => rl,
            Err(e) => {
                eprintln!("Failed to create line editor: {e}");
                return;
            }
        };

        let mut buf = String::new();

        loop {
            let prompt = if buf.is_empty() { "fluss> " } else { "    -> " };
            match rl.readline(prompt) {
                Ok(line) => {
                    let trimmed = line.trim();

                    // Handle meta-commands
                    if buf.is_empty() {
                        match trimmed {
                            "\\q" | "quit" | "exit" => {
                                println!("Bye!");
                                break;
                            }
                            "\\?" | "help" => {
                                print_help();
                                continue;
                            }
                            _ if trimmed.starts_with("\\dt") => {
                                self.execute_sql("SHOW TABLES;").await;
                                continue;
                            }
                            _ if trimmed.is_empty() => {
                                continue;
                            }
                            _ => {}
                        }
                    }

                    buf.push(' ');
                    buf.push_str(trimmed);

                    // Execute when statement ends with ;
                    if buf.trim_end().ends_with(';') {
                        let sql = buf.trim().to_string();
                        let _ = rl.add_history_entry(&sql);
                        self.execute_sql(&sql).await;
                        buf.clear();
                    }
                }
                Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                    println!("Bye!");
                    break;
                }
                Err(e) => {
                    eprintln!("Readline error: {e}");
                    break;
                }
            }
        }
    }

    /// Execute single SQL statement (for non-interactive mode)
    pub async fn execute_sql(&self, sql: &str) {
        let ctx = self.sql_context();

        // Try to rewrite SQL (SHOW commands, etc.)
        let sql_to_run = if let Some(rewritten) = rewrite_sql(sql, &ctx) {
            rewritten
        } else {
            sql.to_string()
        };

        match self.ctx.sql(&sql_to_run).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
                        println!("OK");
                    } else {
                        match pretty_format_batches(&batches) {
                            Ok(table) => println!("{table}"),
                            Err(e) => eprintln!("Format error: {e}"),
                        }
                    }
                }
                Err(e) => eprintln!("Execution error: {e}"),
            },
            Err(e) => eprintln!("SQL error: {e}"),
        }
    }

    /// Get current SQL context
    fn sql_context(&self) -> SqlContext {
        let state = self.ctx.state();
        let config = state.config().options();

        SqlContext::new(
            config.catalog.default_schema.clone(),
            config.catalog.default_catalog.clone(),
        )
    }
}

fn print_help() {
    println!(
        r#"
Commands:
  SQL statements    End with ';' to execute
  \dt               List tables in current database (SHOW TABLES)
  \q / quit / exit  Quit
  \? / help         Show this help

SQL Statements:
  SELECT, INSERT, CREATE TABLE, etc.
  SHOW TABLES [FROM db]
  SHOW CREATE TABLE table_name
  SHOW PARTITIONS table_name
  SHOW BUCKETS table_name
  SHOW [TABLE] OPTIONS table_name
  DESCRIBE table_name
  SHOW DATABASES

Examples:
  CREATE TABLE my_table (id INT PRIMARY KEY, name STRING);
  INSERT INTO my_table VALUES (1, 'hello');
  SELECT * FROM my_table;
  SHOW PARTITIONS my_table;
  DESCRIBE my_table;
"#
    );
}
