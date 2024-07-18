use std::{error::Error, time::Duration};

use configuration::{get_configuration, BatchSettings, SinkSettings, SourceSettings};
use pg_replicate::pipeline::{
    batching::{data_pipeline::BatchDataPipeline, BatchConfig},
    sinks::bigquery::BigQueryBatchSink,
    sources::postgres::{PostgresSource, TableNamesFrom},
    PipelineAction,
};
use secrecy::ExposeSecret;
use tracing::{error, info};

mod configuration;

// APP_SOURCE__POSTGRES__PASSWORD and APP_SINK__BIGQUERY__PROJECT_ID environment variables must be set
// before running because these are sensitive values which can't be configured in the config files
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(e) = main_impl().await {
        error!("{e}");
    }

    Ok(())
}

async fn main_impl() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let settings = get_configuration()?;

    info!("settings: {settings:#?}");

    let SourceSettings::Postgres {
        host,
        port,
        name,
        username,
        password,
        slot_name,
        publication,
    } = settings.source;

    let postgres_source = PostgresSource::new(
        &host,
        port,
        &name,
        &username,
        password.map(|p| p.expose_secret().clone()),
        Some(slot_name),
        TableNamesFrom::Publication(publication),
    )
    .await?;

    let SinkSettings::BigQuery {
        project_id,
        dataset_id,
        service_account_key,
    } = settings.sink;
    info!("secret: {}", service_account_key.expose_secret());

    let bigquery_sink = BigQueryBatchSink::new_with_key(
        project_id,
        dataset_id,
        service_account_key.expose_secret(),
    )
    .await?;

    let BatchSettings {
        max_size,
        max_fill_secs,
    } = settings.batch;

    let batch_config = BatchConfig::new(max_size, Duration::from_secs(max_fill_secs));
    let mut pipeline = BatchDataPipeline::new(
        postgres_source,
        bigquery_sink,
        PipelineAction::Both,
        batch_config,
    );

    pipeline.start().await?;

    Ok(())
}
