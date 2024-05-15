use std::error::Error;

use clap::{Args, Parser, Subcommand};
use futures::StreamExt;
use pg_replicate::{
    conversion::{
        CdcMessage, JsonConversionError, ReplicationMsgJsonConversionError,
        ReplicationMsgToCdcMsgConverter, TableRowToJsonConverter,
    },
    pipeline::sources::{
        postgres::{PostgresSource, TableNamesFrom},
        Source,
    },
    table::TableName,
};
use tokio::pin;
use tokio_postgres::types::PgLsn;
use tracing::{error, info};

#[derive(Debug, Parser)]
#[command(name = "stdout", version, about, arg_required_else_help = true)]
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

    match args.command {
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

            let source: &dyn Source<
                '_,
                '_,
                JsonConversionError,
                TableRowToJsonConverter,
                ReplicationMsgJsonConversionError,
                ReplicationMsgToCdcMsgConverter,
            > = &postgres_source as &dyn Source<_, _, _, _>;

            let table_schemas = source.get_table_schemas();
            let converter = TableRowToJsonConverter;

            for table_schema in table_schemas.values() {
                let table_rows = source
                    .get_table_copy_stream(
                        &table_schema.table_name,
                        &table_schema.column_schemas,
                        &converter,
                    )
                    .await?;

                pin!(table_rows);

                while let Some(row) = table_rows.next().await {
                    let row = row?;
                    info!("row in json format: {row}");
                }
            }
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

            let source: &dyn Source<
                '_,
                '_,
                JsonConversionError,
                TableRowToJsonConverter,
                ReplicationMsgJsonConversionError,
                ReplicationMsgToCdcMsgConverter,
            > = &postgres_source as &dyn Source<_, _, _, _>;

            let table_schemas = source.get_table_schemas();
            let converter = TableRowToJsonConverter;

            for table_schema in table_schemas.values() {
                let table_rows = source
                    .get_table_copy_stream(
                        &table_schema.table_name,
                        &table_schema.column_schemas,
                        &converter,
                    )
                    .await?;

                pin!(table_rows);

                while let Some(row) = table_rows.next().await {
                    let row = row?;
                    info!("row in json format: {row}");
                }
            }

            let converter = ReplicationMsgToCdcMsgConverter;

            let mut last_lsn = PgLsn::from(0);

            let cdc_events = source.get_cdc_stream(last_lsn, &converter).await?;

            pin!(cdc_events);

            while let Some(cdc_event) = cdc_events.next().await {
                let cdc_event = cdc_event?;
                match cdc_event {
                    CdcMessage::Begin(msg) => {
                        info!("Begin: {msg}");
                    }
                    CdcMessage::Commit { lsn, body } => {
                        last_lsn = lsn;
                        info!("Commit: {body}");
                    }
                    CdcMessage::Insert(msg) => {
                        info!("Insert: {msg}");
                    }
                    CdcMessage::Update(msg) => {
                        info!("Update: {msg}");
                    }
                    CdcMessage::Delete(msg) => {
                        info!("Delete: {msg}");
                    }
                    CdcMessage::Relation(msg) => {
                        info!("Relation: {msg}");
                    }
                    CdcMessage::KeepAliveRequested { reply } => {
                        if reply {
                            cdc_events.as_mut().send_status_update(last_lsn).await?;
                        }
                        info!("got keep alive msg")
                    }
                }
            }
        }
    }

    Ok(())
}
