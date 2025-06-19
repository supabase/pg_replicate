use anyhow::anyhow;
use config::replicator::{DestinationConfig, ReplicatorConfig, StateStoreConfig};
use etl::v2::config::batch::BatchConfig;
use etl::v2::config::pipeline::PipelineConfig;
use etl::v2::config::retry::RetryConfig;
use etl::v2::destination::base::Destination;
use etl::v2::destination::memory::MemoryDestination;
use etl::v2::pipeline::{Pipeline, PipelineIdentity};
use etl::v2::state::store::base::StateStore;
use etl::v2::state::store::memory::MemoryStateStore;
use etl::SslMode;
use postgres::tokio::config::PgConnectionConfig;
use std::fmt;
use std::io::BufReader;
use std::time::Duration;
use thiserror::Error;
use tracing::instrument;

use crate::config::load_config;

#[derive(Debug, Error)]
pub enum ReplicatorError {
    #[error("The destination {0} is currently unsupported")]
    UnsupportedDestination(String),
}

#[instrument(name = "start_replicator")]
async fn start_replicator() -> anyhow::Result<()> {
    let replicator_config = load_config()?;

    // We set up the certificates and SSL mode.
    let mut trusted_root_certs = vec![];
    let ssl_mode = if replicator_config.tls.enabled {
        let mut root_certs_reader =
            BufReader::new(replicator_config.tls.trusted_root_certs.as_bytes());
        for cert in rustls_pemfile::certs(&mut root_certs_reader) {
            let cert = cert?;
            trusted_root_certs.push(cert);
        }

        SslMode::VerifyFull
    } else {
        SslMode::Disable
    };

    // We initialize the state store and destination.
    let state_store = init_state_store(&replicator_config).await?;
    let destination = init_destination(&replicator_config).await?;

    // We create the identity of this pipeline.
    let identity = PipelineIdentity::new(
        replicator_config.pipeline_id,
        &replicator_config.source.publication,
    );

    // We prepare the configuration of the pipeline.
    let pipeline_config = PipelineConfig {
        pg_connection: PgConnectionConfig {
            host: replicator_config.source.host,
            port: replicator_config.source.port,
            name: replicator_config.source.name,
            username: replicator_config.source.username,
            password: replicator_config.source.password,
            ssl_mode,
        },
        batch: BatchConfig {
            max_size: replicator_config.batch.max_size,
            max_fill: Duration::from_secs(replicator_config.batch.max_fill_secs),
        },
        // TODO: add support for retry config.
        apply_worker_initialization_retry: RetryConfig::default(),
    };

    let pipeline = Pipeline::new(
        identity,
        pipeline_config,
        trusted_root_certs,
        state_store,
        destination,
    );
    start_pipeline(pipeline).await?;

    Ok(())
}

async fn init_state_store(
    config: &ReplicatorConfig,
) -> anyhow::Result<impl StateStore + Clone + Send + Sync + fmt::Debug + 'static> {
    match config.state_store {
        StateStoreConfig::Memory => Ok(MemoryStateStore::new()),
    }
}

async fn init_destination(
    config: &ReplicatorConfig,
) -> anyhow::Result<impl Destination + Clone + Send + Sync + fmt::Debug + 'static> {
    match config.destination {
        DestinationConfig::Memory => Ok(MemoryDestination::new()),
        _ => {
            Err(ReplicatorError::UnsupportedDestination(format!("{:?}", config.destination)).into())
        }
    }
}

#[instrument(name = "start_pipeline")]
async fn start_pipeline<S, D>(mut pipeline: Pipeline<S, D>) -> anyhow::Result<()>
where
    S: StateStore + Clone + Send + Sync + fmt::Debug + 'static,
    D: Destination + Clone + Send + Sync + fmt::Debug + 'static,
{
    // We start the pipeline.
    pipeline.start().await?;

    // We wait for the pipeline to finish normally or for a `CTRL + C` signal.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            // We send a shutdown signal to the pipeline and wait.
            // pipeline.shutdown_and_wait().await?;
            return Ok(());
        }

        _ = pipeline.wait() => {

        }
    }

    Ok(())
}
