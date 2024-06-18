use std::error::Error;

use clap::{Args, Parser, Subcommand};
use pg_replicate::{
    pipeline::{
        data_pipeline::DataPipeline,
        sinks::bigquery::BigQuerySink,
        sources::postgres::{PostgresSource, TableNamesFrom},
        PipelineAction,
    },
    table::TableName,
};
use tracing::error;

#[derive(Debug, Parser)]
#[command(name = "bigquery", version, about, arg_required_else_help = true)]
struct AppArgs {
    #[clap(flatten)]
    db_args: DbArgs,

    #[clap(flatten)]
    bq_args: BqArgs,

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

#[derive(Debug, Args)]
struct BqArgs {
    /// Path to GCP's service account key to access BigQuery
    #[arg(long)]
    bq_sa_key_file: String,

    /// BigQuery project id
    #[arg(long)]
    bq_project_id: String,

    /// BigQuery dataset id
    #[arg(long)]
    bq_dataset_id: String,
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
    let bq_args = args.bq_args;

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
            (postgres_source, PipelineAction::TableCopiesOnly)
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

            (postgres_source, PipelineAction::Both)
        }
    };

    let bigquery_sink = BigQuerySink::new(
        bq_args.bq_project_id,
        bq_args.bq_dataset_id,
        &bq_args.bq_sa_key_file,
    )
    .await?;

    let mut pipeline = DataPipeline::new(postgres_source, bigquery_sink, action);

    pipeline.start().await?;

    Ok(())
}
