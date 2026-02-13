mod catalog;
mod cli;
mod error;
mod provider;

use crate::catalog::FlussCatalog;
use crate::cli::FlussCliSession;
use clap::Parser;
use nu_ansi_term::Color;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(
    name = "fluss-datafusion",
    about = "Interactive SQL CLI for Apache Fluss"
)]
struct Args {
    /// Fluss bootstrap server address
    #[arg(short, long, default_value = "127.0.0.1:9123")]
    bootstrap_server: String,

    /// Default database to use
    #[arg(short, long, default_value = "fluss")]
    database: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logforth::stderr().apply();

    let banner = r#"

        ███████╗██╗     ██╗   ██╗███████╗███████╗
        ██╔════╝██║     ██║   ██║██╔════╝██╔════╝
        █████╗  ██║     ██║   ██║███████╗███████╗
        ██╔══╝  ██║     ██║   ██║╚════██║╚════██║
        ██║     ███████╗╚██████╔╝███████║███████║
        ╚═╝     ╚══════╝ ╚═════╝ ╚══════╝╚══════╝
"#;
    println!("{}", Color::Cyan.paint(banner));

    let args = Args::parse();

    println!("Fluss SQL CLI (powered by Apache DataFusion)");
    println!("Connecting to {}...", args.bootstrap_server);

    let mut config = fluss::config::Config::default();
    config.bootstrap_servers = args.bootstrap_server.to_owned();

    let conn = fluss::client::FlussConnection::new(config).await?;
    let conn = Arc::new(conn);

    let catalog = FlussCatalog::new(conn.clone(), args.database.to_owned());

    let ctx = datafusion::prelude::SessionContext::new();
    ctx.register_catalog("fluss", Arc::new(catalog));

    // Set fluss as the default catalog and the specified database as default schema.
    ctx.sql(&format!("SET datafusion.catalog.default_catalog = 'fluss'"))
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
    println!("Type SQL statements or \\q to quit.\n");

    let mut session = FlussCliSession::new(ctx, conn);
    session.run().await;

    Ok(())
}
