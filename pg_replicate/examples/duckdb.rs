use std::error::Error;

use clap::{Args, Parser, Subcommand};
use pg_replicate::{
    conversions::json::{ReplicationMsgJsonConversionError, ReplicationMsgToCdcMsgConverter},
    pipeline::{
        sinks::duckdb::DuckDbSink,
        sources::postgres::{PostgresSource, TableNamesFrom},
        DataPipeline, PipelinAction,
    },
    table::TableName,
};
use tracing::error;

#[derive(Debug, Parser)]
#[command(name = "duckdb", version, about, arg_required_else_help = true)]
struct AppArgs {
    #[clap(flatten)]
    db_args: DbArgs,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Args)]
struct DbArgs {
    /// Host on which Postgres is running
    #[arg(long)]
    db_host: String,

    /// Port on which Postgres is running
    #[arg(long)]
    db_port: u16,

    /// Postgres database name
    #[arg(long)]
    db_name: String,

    /// Postgres database user name
    #[arg(long)]
    db_username: String,

    /// Postgres database user password
    #[arg(long)]
    db_password: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Copy a table
    CopyTable { schema: String, name: String },

    /// Start a change data capture
    Cdc {
        publication: String,
        slot_name: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(e) = main_impl().await {
        error!("{e}");
    }

    Ok(())
}

async fn main_impl() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let args = AppArgs::parse();
    let db_args = args.db_args;

    let (postgres_source, action) = match args.command {
        Command::CopyTable { schema, name } => {
            let table_names = vec![TableName { schema, name }];

            let postgres_source = PostgresSource::new(
                &db_args.db_host,
                db_args.db_port,
                &db_args.db_name,
                &db_args.db_username,
                db_args.db_password,
                None,
                TableNamesFrom::Vec(table_names),
            )
            .await?;
            (postgres_source, PipelinAction::TableCopiesOnly)
        }
        Command::Cdc {
            publication,
            slot_name,
        } => {
            let postgres_source = PostgresSource::new(
                &db_args.db_host,
                db_args.db_port,
                &db_args.db_name,
                &db_args.db_username,
                db_args.db_password,
                Some(slot_name),
                TableNamesFrom::Publication(publication),
            )
            .await?;

            (postgres_source, PipelinAction::Both)
        }
    };

    let duckdb_sink = DuckDbSink::new().await?;
    let cdc_converter = ReplicationMsgToCdcMsgConverter;

    let pipeline: DataPipeline<
        '_,
        '_,
        ReplicationMsgJsonConversionError,
        ReplicationMsgToCdcMsgConverter,
        PostgresSource,
        DuckDbSink,
    > = DataPipeline::new(postgres_source, duckdb_sink, action, cdc_converter);

    pipeline.start().await?;

    Ok(())
}
