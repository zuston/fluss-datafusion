//! CLI Interactive SQL Session
//!
//! Provides interactive REPL, supporting:
//! - Standard SQL statement execution
//! - Fluss-specific SHOW/DESCRIBE commands
//! - Meta-commands (\dt, \q, \?)
//! - Persistent readline history across CLI sessions

use std::path::{Path, PathBuf};
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
        let history_path = repl_history_file();

        let mut rl = match DefaultEditor::new() {
            Ok(rl) => rl,
            Err(e) => {
                eprintln!("Failed to create line editor: {e}");
                return;
            }
        };

        if let Some(ref path) = history_path {
            if path.exists() {
                if let Err(e) = rl.load_history(path) {
                    eprintln!(
                        "Warning: could not load CLI history ({}): {e}",
                        path.display()
                    );
                }
            }
        }

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

        if let Some(ref path) = history_path {
            save_repl_history(&mut rl, path);
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
                        let to_show = match crate::cli_display::prepare_batches_for_terminal(
                            &batches,
                        ) {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("Warning: nested column formatting failed ({e}); using default table layout.");
                                batches.clone()
                            }
                        };
                        match pretty_format_batches(&to_show) {
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

/// Default location: `~/.fluss-datafusion/repl_history` (or `%USERPROFILE%\.fluss-datafusion\repl_history` on Windows).
fn repl_history_file() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("USERPROFILE").map(PathBuf::from))?;
    Some(home.join(".fluss-datafusion").join("repl_history"))
}

fn save_repl_history(rl: &mut DefaultEditor, path: &Path) {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                eprintln!(
                    "Warning: could not create CLI history directory ({}): {e}",
                    parent.display()
                );
                return;
            }
        }
    }
    if let Err(e) = rl.save_history(path) {
        eprintln!(
            "Warning: could not save CLI history ({}): {e}",
            path.display()
        );
    }
}

fn print_help() {
    println!(
        r#"
Commands:
  SQL statements    End with ';' to execute (↑/↓ for history from past sessions)
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
