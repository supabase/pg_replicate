use std::{error::Error, time::Duration};

use configuration::{get_configuration, BatchSettings, SinkSettings, SourceSettings};
use pg_replicate::pipeline::{
    batching::{data_pipeline::BatchDataPipeline, BatchConfig},
    sinks::bigquery::BigQueryBatchSink,
    sources::postgres::{PostgresSource, TableNamesFrom},
    PipelineAction,
};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

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

fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "replicator=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

async fn main_impl() -> Result<(), Box<dyn Error>> {
    init_tracing();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

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
        password,
        Some(slot_name),
        TableNamesFrom::Publication(publication),
    )
    .await?;

    let SinkSettings::BigQuery {
        project_id,
        dataset_id,
        service_account_key,
    } = settings.sink;

    let bigquery_sink =
        BigQueryBatchSink::new_with_key(project_id, dataset_id, &service_account_key).await?;

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
