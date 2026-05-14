//! Fluss DataFusion SQL CLI
//!
//! This project is a DataFusion integration for Apache Fluss, providing an interactive SQL CLI tool.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                         CLI Layer                            │
//! │                   (cli.rs - Interactive REPL)                │
//! ├─────────────────────────────────────────────────────────────┤
//! │                      SQL Extension Layer                   │
//! │           (sql/ - Dialect, Rewriter, information_schema)     │
//! ├─────────────────────────────────────────────────────────────┤
//! │                      Catalog Layer                         │
//! │        (catalog/ - CatalogProvider/SchemaProvider)         │
//! ├─────────────────────────────────────────────────────────────┤
//! │                      Provider Layer                        │
//! │       (provider/ - TableProvider/ExecutionPlan)            │
//! ├─────────────────────────────────────────────────────────────┤
//! │                      Fluss Client                          │
//! │              (fluss-rs - Fluss cluster communication)      │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use std::sync::Arc;

use clap::Parser;
use nu_ansi_term::Color;

mod catalog;
mod cli;
mod cli_display;
mod error;
mod provider;
mod sql;

use crate::catalog::FlussCatalog;
use crate::cli::FlussCliSession;

/// Fluss DataFusion CLI command line arguments
#[derive(Parser, Debug)]
#[command(
    name = "fluss-datafusion",
    about = "Interactive SQL CLI for Apache Fluss",
    version
)]
struct Args {
    /// Fluss bootstrap server address
    #[arg(short, long, default_value = "127.0.0.1:9123")]
    bootstrap_server: String,

    /// Default database to use
    #[arg(short, long, default_value = "fluss")]
    database: String,

    /// Non-interactive mode: execute a single SQL statement and exit
    #[arg(short, long)]
    execute: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    logforth::stderr().apply();

    print_banner();

    let args = Args::parse();

    println!("Fluss SQL CLI (powered by Apache DataFusion)");
    println!("Connecting to {}...", args.bootstrap_server);

    // Initialize Fluss connection
    let mut config = fluss::config::Config::default();
    config.bootstrap_servers = args.bootstrap_server;

    let conn = fluss::client::FlussConnection::new(config).await?;
    let conn = Arc::new(conn);

    // Create Fluss catalog with SQL context
    let catalog = FlussCatalog::new(conn.clone(), args.database.clone());

    // Initialize DataFusion context
    let ctx = datafusion::prelude::SessionContext::new();
    ctx.register_catalog("fluss", Arc::new(catalog));

    // Set default catalog and schema
    ctx.sql("SET datafusion.catalog.default_catalog = 'fluss'")
        .await?
        .collect()
        .await?;
    ctx.sql(&format!(
        "SET datafusion.catalog.default_schema = '{}'",
        args.database
    ))
    .await?
    .collect()
    .await?;

    println!("Connected. Default database: {}", args.database);
    println!("Type SQL statements or \\? for help.\n");

    // Execute non-interactive mode or start REPL
    if let Some(sql) = args.execute {
        // Single statement mode
        let session = FlussCliSession::new(ctx, conn);
        session.execute_sql(&sql).await;
    } else {
        // Interactive REPL mode
        let mut session = FlussCliSession::new(ctx, conn);
        session.run().await;
    }

    Ok(())
}

fn print_banner() {
    let banner = r#"

        ███████╗██╗     ██╗   ██╗███████╗███████╗
        ██╔════╝██║     ██║   ██║██╔════╝██╔════╝
        █████╗  ██║     ██║   ██║███████╗███████╗
        ██╔══╝  ██║     ██║   ██║╚════██║╚════██║
        ██║     ███████╗╚██████╔╝███████║███████║
        ╚═╝     ╚══════╝ ╚═════╝ ╚══════╝╚══════╝
"#;
    println!("{}", Color::Cyan.paint(banner));
}
