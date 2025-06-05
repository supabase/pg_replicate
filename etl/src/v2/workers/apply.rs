use crate::v2::config::pipeline::PipelineConfig;
use crate::v2::destination::base::Destination;
use crate::v2::pipeline::PipelineIdentity;
use crate::v2::replication::apply::{start_apply_loop, ApplyLoopError, ApplyLoopHook};
use crate::v2::replication::client::PgReplicationClient;
use crate::v2::replication::slot::SlotUsage;
use crate::v2::state::origin::ReplicationOriginState;
use crate::v2::state::store::base::{StateStore, StateStoreError};
use crate::v2::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::v2::workers::base::{Worker, WorkerHandle, WorkerWaitError};
use crate::v2::workers::pool::TableSyncWorkerPool;
use crate::v2::workers::table_sync::{
    TableSyncWorker, TableSyncWorkerError, TableSyncWorkerStateError,
};
use postgres::schema::Oid;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;
use tracing::{error, info};

#[derive(Debug, Error)]
pub enum ApplyWorkerError {
    #[error("An error occurred while interacting with the state store: {0}")]
    StateStoreError(#[from] StateStoreError),

    #[error("An error occurred in the apply loop: {0}")]
    ApplyLoop(#[from] ApplyLoopError),
}

#[derive(Debug, Error)]
pub enum ApplyWorkerHookError {
    #[error("An error occurred while interacting with the state store: {0}")]
    StateStoreError(#[from] StateStoreError),

    #[error("An error occurred while interacting with the table sync worker state: {0}")]
    TableSyncWorkerState(#[from] TableSyncWorkerStateError),

    #[error("An error occurred while trying to start the table sync worker: {0}")]
    TableSyncWorkerStartedFailed(#[from] TableSyncWorkerError),
}

#[derive(Debug)]
pub struct ApplyWorkerHandle {
    handle: Option<JoinHandle<Result<(), ApplyWorkerError>>>,
}

impl WorkerHandle<()> for ApplyWorkerHandle {
    fn state(&self) {}

    async fn wait(mut self) -> Result<(), WorkerWaitError> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };

        handle.await??;

        Ok(())
    }
}

#[derive(Debug)]
pub struct ApplyWorker<S, D> {
    identity: PipelineIdentity,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    pool: TableSyncWorkerPool,
    state_store: S,
    destination: D,
    shutdown_rx: watch::Receiver<()>,
}

impl<S, D> ApplyWorker<S, D> {
    pub fn new(
        identity: PipelineIdentity,
        config: Arc<PipelineConfig>,
        replication_client: PgReplicationClient,
        pool: TableSyncWorkerPool,
        state_store: S,
        destination: D,
        shutdown_rx: watch::Receiver<()>,
    ) -> Self {
        Self {
            identity,
            config,
            replication_client,
            pool,
            state_store,
            destination,
            shutdown_rx,
        }
    }
}

impl<S, D> Worker<ApplyWorkerHandle, ()> for ApplyWorker<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    type Error = ApplyWorkerError;

    async fn start(self) -> Result<ApplyWorkerHandle, Self::Error> {
        info!("Starting apply worker");

        let apply_worker = async move {
            // TODO: get the slot of the main apply worker or create it if needed.
            // We load or initialize the initial state that will be used for the apply worker.
            let replication_origin_state = match self
                .state_store
                .load_replication_origin_state(self.identity.id(), None)
                .await?
            {
                Some(replication_origin_state) => replication_origin_state,
                None => {
                    // TODO: use the consistent point from the slot during creation here.
                    let replication_origin_state =
                        ReplicationOriginState::new(self.identity.id(), None, PgLsn::from(0));
                    self.state_store
                        .store_replication_origin_state(replication_origin_state.clone(), true)
                        .await?;

                    replication_origin_state
                }
            };

            let hook = Hook::new(
                self.identity.clone(),
                self.config.clone(),
                self.replication_client.clone(),
                self.pool,
                self.state_store.clone(),
                self.destination.clone(),
                self.shutdown_rx.clone(),
            );

            start_apply_loop(
                self.identity,
                replication_origin_state.remote_lsn,
                self.config,
                self.replication_client,
                self.state_store,
                self.destination,
                hook,
                self.shutdown_rx,
            )
            .await?;

            Ok(())
        };

        let handle = tokio::spawn(apply_worker);

        Ok(ApplyWorkerHandle {
            handle: Some(handle),
        })
    }
}

#[derive(Debug)]
struct Hook<S, D> {
    identity: PipelineIdentity,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    pool: TableSyncWorkerPool,
    state_store: S,
    destination: D,
    shutdown_rx: watch::Receiver<()>,
}

impl<S, D> Hook<S, D> {
    fn new(
        identity: PipelineIdentity,
        config: Arc<PipelineConfig>,
        replication_client: PgReplicationClient,
        pool: TableSyncWorkerPool,
        state_store: S,
        destination: D,
        shutdown_rx: watch::Receiver<()>,
    ) -> Self {
        Self {
            identity,
            config,
            replication_client,
            pool,
            state_store,
            destination,
            shutdown_rx,
        }
    }
}

impl<S, D> ApplyLoopHook for Hook<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    type Error = ApplyWorkerHookError;

    async fn process_syncing_tables(
        &self,
        current_lsn: PgLsn,
        initial_sync: bool,
    ) -> Result<(), Self::Error> {
        let table_replication_states = self.state_store.load_table_replication_states().await?;
        info!(
            "Processing syncing tables for apply worker with LSN {}",
            current_lsn
        );

        for table_replication_state in table_replication_states {
            if !initial_sync {
                if let TableReplicationPhase::SyncDone { lsn } = table_replication_state.phase {
                    if current_lsn >= lsn {
                        let table_replication_state = table_replication_state
                            .with_phase(TableReplicationPhase::Ready { lsn: current_lsn });
                        self.state_store
                            .store_table_replication_state(table_replication_state, true)
                            .await?;
                    }
                }
            } else {
                {
                    let pool = self.pool.read().await;
                    if let Some(table_sync_worker_state) =
                        pool.get_worker_state(table_replication_state.table_id)
                    {
                        if !initial_sync {
                            let mut catchup_started = false;
                            let mut inner = table_sync_worker_state.get_inner().write().await;
                            if inner.replication_phase().as_type()
                                == TableReplicationPhaseType::SyncWait
                            {
                                inner
                                    .set_phase_with(
                                        TableReplicationPhase::Catchup { lsn: current_lsn },
                                        self.state_store.clone(),
                                    )
                                    .await?;
                                catchup_started = true;
                            }
                            drop(inner);

                            if catchup_started {
                                println!("WAITING FOR SYNC DONE");
                                let _ = table_sync_worker_state
                                    .wait_for_phase_type(TableReplicationPhaseType::SyncDone)
                                    .await;
                                println!("SYNC DONE RECEIVED");
                            }

                            continue;
                        }
                    }
                }

                info!(
                    "Creating new sync worker for table {}",
                    table_replication_state.table_id
                );

                match self.replication_client.duplicate().await {
                    Ok(duplicate_replication_client) => {
                        let worker = TableSyncWorker::new(
                            self.identity.clone(),
                            self.config.clone(),
                            duplicate_replication_client,
                            self.pool.clone(),
                            table_replication_state.table_id,
                            self.state_store.clone(),
                            self.destination.clone(),
                            self.shutdown_rx.clone(),
                        );

                        let mut pool = self.pool.write().await;
                        if let Err(err) = pool.start_worker(worker).await {
                            // TODO: check if we want to build a backoff mechanism for retrying the
                            //  spawning of new table sync workers.
                            error!(
                                "Failed to start table sync worker, retrying in next loop: {}",
                                err
                            );
                        }
                    }
                    Err(err) => {
                        error!("Failed to create new sync worker because the replication client couldn't be duplicated: {}", err);
                    }
                }
            }
        }

        Ok(())
    }

    async fn should_apply_changes(&self, table_id: Oid, remote_final_lsn: PgLsn) -> bool {
        let pool = self.pool.read().await;
        let Some(table_sync_worker_state) = pool.get_worker_state(table_id) else {
            return false;
        };

        let inner = table_sync_worker_state.get_inner().read().await;
        match inner.replication_phase() {
            TableReplicationPhase::Ready { .. } => true,
            TableReplicationPhase::SyncDone { lsn } => lsn <= remote_final_lsn,
            _ => false,
        }
    }

    fn slot_usage(&self) -> SlotUsage {
        SlotUsage::ApplyWorker
    }
}
