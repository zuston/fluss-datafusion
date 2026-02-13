use std::sync::Arc;

use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use fluss::client::FlussConnection;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

/// Interactive SQL session backed by DataFusion + Fluss.
pub struct FlussCliSession {
    ctx: SessionContext,
}

impl FlussCliSession {
    pub fn new(ctx: SessionContext, _conn: Arc<FlussConnection>) -> Self {
        Self { ctx }
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
                            self.execute_sql("SHOW TABLES;").await;
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
        let rewritten = rewrite_show_tables_sql(sql, self.current_database());
        let sql_to_run = rewritten.as_deref().unwrap_or(sql);

        match self.ctx.sql(sql_to_run).await {
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

    fn current_database(&self) -> String {
        self.ctx
            .state()
            .config()
            .options()
            .catalog
            .default_schema
            .clone()
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

fn parse_show_tables(sql: &str) -> Option<Option<String>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let original_tokens: Vec<&str> = trimmed.split_whitespace().collect();
    let tokens: Vec<String> = original_tokens
        .iter()
        .map(|t| t.to_ascii_lowercase())
        .collect();

    if tokens.len() == 2 && tokens[0] == "show" && tokens[1] == "tables" {
        return Some(None);
    }

    if tokens.len() == 4
        && tokens[0] == "show"
        && tokens[1] == "tables"
        && (tokens[2] == "from" || tokens[2] == "in")
    {
        return Some(Some(original_tokens[3].to_string()));
    }

    None
}

fn rewrite_show_tables_sql(sql: &str, current_db: String) -> Option<String> {
    let database = parse_show_tables(sql)?
        .map(|db| trim_identifier_quotes(&db).to_string())
        .unwrap_or(current_db);
    let escaped_db = database.replace('\'', "''");

    Some(format!(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = '{escaped_db}' ORDER BY table_name"
    ))
}

fn trim_identifier_quotes(input: &str) -> &str {
    input
        .strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
        .or_else(|| input.strip_prefix('"').and_then(|s| s.strip_suffix('"')))
        .unwrap_or(input)
}
