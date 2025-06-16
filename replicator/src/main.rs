use std::{io::BufReader, time::Duration, vec};

use configuration::{
    get_configuration, BatchSettings, DestinationSettings, Settings, SourceSettings, TlsSettings,
};
use etl::{
    pipeline::{
        batching::{data_pipeline::BatchDataPipeline, BatchConfig},
        destinations::bigquery::BigQueryBatchDestination,
        sources::postgres::{PostgresSource, TableNamesFrom},
        PipelineAction,
    },
    SslMode,
};
use postgres::tokio::config::PgConnectionConfig;
use telemetry::init_tracing;
use tracing::{info, instrument};

use crate::migrations::migrate_source_database;

mod configuration;
mod migrations;

// APP_SOURCE__POSTGRES__PASSWORD and APP_DESTINATION__BIG_QUERY__PROJECT_ID environment variables must be set
// before running because these are sensitive values which can't be configured in the config files
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app_name = env!("CARGO_BIN_NAME");
    // We pass emit_on_span_close = false to avoid emitting logs on span close
    // for replicator because it is not a web server and we don't need to emit logs
    // for every closing span.
    let _log_flusher = init_tracing(app_name, false)?;
    let settings = get_configuration()?;
    start_replication(settings).await
}

#[instrument(name = "replication", skip(settings), fields(project = settings.project))]
async fn start_replication(settings: Settings) -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    migrate_source_database(&settings.source, &settings.tls).await?;

    let SourceSettings::Postgres {
        host,
        port,
        name,
        username,
        password: _,
        slot_name,
        publication,
    } = &settings.source;
    info!(
        host,
        port,
        dbname = name,
        username,
        slot_name,
        publication,
        "source settings"
    );

    let DestinationSettings::BigQuery {
        project_id,
        dataset_id,
        service_account_key: _,
        max_staleness_mins,
    } = &settings.destination;

    info!(
        project_id,
        dataset_id, max_staleness_mins, "destination settings"
    );

    let BatchSettings {
        max_size,
        max_fill_secs,
    } = &settings.batch;
    info!(max_size, max_fill_secs, "batch settings");

    let TlsSettings {
        trusted_root_certs: _,
        enabled,
    } = &settings.tls;
    info!(tls_enabled = enabled, "tls settings");

    settings.tls.validate()?;

    let SourceSettings::Postgres {
        host,
        port,
        name,
        username,
        password,
        slot_name,
        publication,
    } = settings.source;

    let TlsSettings {
        trusted_root_certs,
        enabled,
    } = settings.tls;

    let mut trusted_root_certs_vec = vec![];
    let ssl_mode = if enabled {
        let mut root_certs_reader = BufReader::new(trusted_root_certs.as_bytes());
        for cert in rustls_pemfile::certs(&mut root_certs_reader) {
            let cert = cert?;
            trusted_root_certs_vec.push(cert);
        }

        SslMode::VerifyFull
    } else {
        SslMode::Disable
    };

    let options = PgConnectionConfig {
        host,
        port,
        name,
        username,
        password,
        ssl_mode,
    };

    let postgres_source = PostgresSource::new(
        options,
        trusted_root_certs_vec,
        Some(slot_name),
        TableNamesFrom::Publication(publication),
    )
    .await?;

    let DestinationSettings::BigQuery {
        project_id,
        dataset_id,
        service_account_key,
        max_staleness_mins,
    } = settings.destination;

    let bigquery_destination = BigQueryBatchDestination::new_with_key(
        project_id,
        dataset_id,
        &service_account_key,
        max_staleness_mins.unwrap_or(5),
    )
    .await?;

    let BatchSettings {
        max_size,
        max_fill_secs,
    } = settings.batch;

    let batch_config = BatchConfig::new(max_size, Duration::from_secs(max_fill_secs));
    let mut pipeline = BatchDataPipeline::new(
        postgres_source,
        bigquery_destination,
        PipelineAction::Both,
        batch_config,
    );

    pipeline.start().await?;

    Ok(())
}
