use std::sync::Arc;

use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use fluss::client::FlussConnection;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

/// Interactive SQL session backed by DataFusion + Fluss.
pub struct FlussCliSession {
    ctx: SessionContext,
    conn: Arc<FlussConnection>,
}

impl FlussCliSession {
    pub fn new(ctx: SessionContext, conn: Arc<FlussConnection>) -> Self {
        Self { ctx, conn }
    }

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

                    // Meta commands.
                    if buf.is_empty() {
                        if trimmed == "\\q" || trimmed == "quit" || trimmed == "exit" {
                            println!("Bye!");
                            break;
                        }
                        if trimmed == "\\?" || trimmed == "help" {
                            print_help();
                            continue;
                        }
                        if trimmed.starts_with("\\dt") {
                            self.show_tables().await;
                            continue;
                        }
                        if trimmed.is_empty() {
                            continue;
                        }
                    }

                    buf.push(' ');
                    buf.push_str(trimmed);

                    // If the statement ends with `;`, execute it.
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

    async fn execute_sql(&self, sql: &str) {
        match self.ctx.sql(sql).await {
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

    async fn show_tables(&self) {
        let admin = match self.conn.get_admin().await {
            Ok(a) => a,
            Err(e) => {
                eprintln!("Failed to get admin: {e}");
                return;
            }
        };

        // Determine current database from DataFusion config.
        let db = self
            .ctx
            .state()
            .config()
            .options()
            .catalog
            .default_schema
            .clone();

        match admin.list_tables(&db).await {
            Ok(tables) => {
                if tables.is_empty() {
                    println!("No tables in database '{db}'.");
                } else {
                    println!("Tables in '{db}':");
                    for t in &tables {
                        println!("  {t}");
                    }
                }
            }
            Err(e) => eprintln!("Error listing tables: {e}"),
        }
    }
}

fn print_help() {
    println!(
        r#"
Commands:
  SQL statements    End with ';' to execute
  \dt               List tables in current database
  \q / quit / exit  Quit
  \? / help         Show this help
"#
    );
}
