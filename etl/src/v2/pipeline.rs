use rustls::pki_types::CertificateDer;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;
use tokio_postgres::config::SslMode;
use tracing::{error, info};

use crate::v2::config::pipeline::PipelineConfig;
use crate::v2::destination::base::Destination;
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::state::store::base::{StateStore, StateStoreError};
use crate::v2::state::table::TableReplicationState;
use crate::v2::workers::apply::{ApplyWorker, ApplyWorkerError, ApplyWorkerHandle};
use crate::v2::workers::base::{Worker, WorkerHandle, WorkerWaitError};
use crate::v2::workers::pool::TableSyncWorkerPool;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("Worker operation failed: {0}")]
    WorkerError(#[from] WorkerWaitError),

    #[error("PostgreSQL replication operation failed: {0}")]
    PgReplicationClient(#[from] PgReplicationError),

    #[error("Apply worker failed to start in the pipeline: {0}")]
    ApplyWorker(#[from] ApplyWorkerError),

    #[error("An error happened in the state store: {0}")]
    StateStore(#[from] StateStoreError),
}

#[derive(Debug)]
enum PipelineWorkers {
    NotStarted,
    Started {
        // TODO: investigate whether we could benefit from a central launcher that deals at a high-level
        //  with workers management, which should not be done in the pipeline.
        apply_worker: ApplyWorkerHandle,
        pool: TableSyncWorkerPool,
    },
}

pub type PipelineId = u64;

#[derive(Debug, Clone)]
pub struct PipelineIdentity {
    id: PipelineId,
    publication_name: String,
}

impl PipelineIdentity {
    pub fn new(id: u64, publication_name: &str) -> Self {
        Self {
            id,
            publication_name: publication_name.to_owned(),
        }
    }

    pub fn id(&self) -> PipelineId {
        self.id
    }

    pub fn publication_name(&self) -> &str {
        &self.publication_name
    }
}

#[derive(Debug)]
pub struct Pipeline<S, D> {
    identity: PipelineIdentity,
    config: Arc<PipelineConfig>,
    trusted_root_certs: Vec<CertificateDer<'static>>,
    state_store: S,
    destination: D,
    workers: PipelineWorkers,
    shutdown_tx: watch::Sender<()>,
}

impl<S, D> Pipeline<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    pub fn new(
        identity: PipelineIdentity,
        config: PipelineConfig,
        trusted_root_certs: Vec<CertificateDer<'static>>,
        state_store: S,
        destination: D,
    ) -> Self {
        // We create a watch channel of unit types since this is just used to notify all subscribers
        // that shutdown is needed.
        let (shutdown_tx, _) = watch::channel(());

        Self {
            identity,
            config: Arc::new(config),
            trusted_root_certs,
            state_store,
            destination,
            workers: PipelineWorkers::NotStarted,
            shutdown_tx,
        }
    }

    pub fn identity(&self) -> &PipelineIdentity {
        &self.identity
    }

    pub async fn start(&mut self) -> Result<(), PipelineError> {
        info!(
            "Starting pipeline for publication {}",
            self.identity.publication_name()
        );

        // We create the first connection to Postgres. Note that other connections will be created
        // by duplicating this first one.
        let replication_client = self.connect().await?;

        // We synchronize the relation subscription states with the publication, to make sure we
        // always know which tables to work with. Maybe in the future we also want to react in real
        // time to new relation ids being sent over by the cdc event stream.
        self.sync_relation_subscription_states(&replication_client)
            .await?;

        // We create the table sync workers pool to manage all table sync workers in a central place.
        let pool = TableSyncWorkerPool::new();

        // We create and start the apply worker.
        let apply_worker = ApplyWorker::new(
            self.identity.clone(),
            self.config.clone(),
            replication_client,
            pool.clone(),
            self.state_store.clone(),
            self.destination.clone(),
            self.shutdown_tx.subscribe(),
        )
        .start()
        .await?;

        self.workers = PipelineWorkers::Started { apply_worker, pool };

        Ok(())
    }

    async fn sync_relation_subscription_states(
        &self,
        replication_client: &PgReplicationClient,
    ) -> Result<(), PipelineError> {
        info!("Synchronizing relation subscription states");

        // We fetch all the table ids for the publication to which the pipeline subscribes.
        let table_ids = replication_client
            .get_publication_table_ids(self.identity.publication_name())
            .await?;
        for table_id in table_ids {
            let state = TableReplicationState::init(self.identity.id, table_id);
            // We store the init state only if it's not already present.
            self.state_store
                .store_table_replication_state(state, false)
                .await?;
        }

        Ok(())
    }

    async fn connect(&self) -> Result<PgReplicationClient, PipelineError> {
        // We create the main replication client that will be used by the apply worker.
        let replication_client = match self.config.pg_database_config.ssl_mode {
            SslMode::Disable => {
                PgReplicationClient::connect_no_tls(self.config.pg_database_config.clone()).await?
            }
            _ => {
                PgReplicationClient::connect_tls(
                    self.config.pg_database_config.clone(),
                    self.trusted_root_certs.clone(),
                )
                .await?
            }
        };

        Ok(replication_client)
    }

    pub async fn wait(self) -> Result<(), Vec<WorkerWaitError>> {
        let PipelineWorkers::Started { apply_worker, pool } = self.workers else {
            info!("Pipeline was not started, nothing to wait for");
            return Ok(());
        };

        info!("Waiting for pipeline workers to complete");

        // We first wait for the apply worker to finish, since that must be done before waiting for
        // the table sync workers to finish, otherwise if we wait for sync workers first, we might
        // be having the apply worker that spawns new sync workers after we waited for the current
        // ones to finish.
        let apply_worker_result = apply_worker.wait().await;
        info!("Apply worker completed");

        let table_sync_workers_result = pool.wait_all().await;
        info!("All table sync workers completed");

        match (apply_worker_result, table_sync_workers_result) {
            (Ok(_), Ok(_)) => Ok(()),
            (Err(err), Ok(_)) => Err(vec![err]),
            (Ok(_), Err(err)) => Err(err),
            (Err(apply_err), Err(mut table_sync_err)) => {
                // For efficiency, we add it to the end.
                table_sync_err.push(apply_err);
                Err(table_sync_err)
            }
        }
    }

    pub async fn shutdown(&self) {
        info!("Shutting down pipeline");
        let _ = self.shutdown_tx.send(());
    }

    pub async fn shutdown_and_wait(self) -> Result<(), Vec<WorkerWaitError>> {
        self.shutdown().await;
        self.wait().await
    }
}
