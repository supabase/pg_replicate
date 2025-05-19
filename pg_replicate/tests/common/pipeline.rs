use pg_replicate::pipeline::batching::data_pipeline::BatchDataPipeline;
use pg_replicate::pipeline::batching::BatchConfig;
use pg_replicate::pipeline::sinks::BatchSink;
use pg_replicate::pipeline::sources::postgres::{PostgresSource, TableNamesFrom};
use pg_replicate::pipeline::PipelineAction;
use postgres::schema::TableName;
use postgres::tokio::options::PgDatabaseOptions;
use postgres::tokio::test_utils::PgDatabase;
use std::time::Duration;
use tokio_postgres::config::SslMode;
use uuid::Uuid;

pub enum PipelineMode {
    /// In this mode the supplied tables will be copied.
    CopyTable { table_names: Vec<TableName> },
    /// In this mode the changes will be consumed from the given publication and slot.
    ///
    /// If the slot is not supplied, a new one will be created on the supplied publication.
    Cdc {
        publication: String,
        slot_name: Option<String>,
    },
}

pub async fn spawn_database_with_publication(
    table_names: Vec<TableName>,
    publication_name: Option<String>,
) -> PgDatabase {
    let options = PgDatabaseOptions {
        host: "localhost".to_owned(),
        port: 540,
        name: Uuid::new_v4().to_string(),
        username: "postgres".to_owned(),
        password: Some("postgres".to_owned()),
        ssl_mode: SslMode::Disable,
    };

    let database = PgDatabase::new(options).await;

    if let Some(publication_name) = publication_name {
        database
            .create_publication(&publication_name, &table_names)
            .await
            .expect("Error while creating a publication");
    }

    database
}

pub async fn spawn_pg_pipeline<Snk: BatchSink>(
    options: &PgDatabaseOptions,
    mode: PipelineMode,
    sink: Snk,
) -> BatchDataPipeline<PostgresSource, Snk> {
    let batch_config = BatchConfig::new(1000, Duration::from_secs(10));

    let pipeline = match mode {
        PipelineMode::CopyTable { table_names } => {
            let source = PostgresSource::new(
                options.clone(),
                vec![],
                None,
                TableNamesFrom::Vec(table_names),
            )
            .await
            .expect("Failure when creating the Postgres source for copying tables");
            let action = PipelineAction::TableCopiesOnly;
            BatchDataPipeline::new(source, sink, action, batch_config)
        }
        PipelineMode::Cdc {
            publication,
            slot_name,
        } => {
            let source = PostgresSource::new(
                options.clone(),
                vec![],
                slot_name,
                TableNamesFrom::Publication(publication),
            )
            .await
            .expect("Failure when creating the Postgres source for cdc");
            let action = PipelineAction::CdcOnly;
            BatchDataPipeline::new(source, sink, action, batch_config)
        }
    };

    pipeline
}
