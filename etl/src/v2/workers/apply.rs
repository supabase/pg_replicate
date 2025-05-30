use crate::v2::destination::base::Destination;
use crate::v2::pipeline::PipelineIdentity;
use crate::v2::replication::apply::{start_apply_loop, ApplyLoopError, ApplyLoopHook};
use crate::v2::replication::client::PgReplicationClient;
use crate::v2::state::store::base::{PipelineStateStore, PipelineStateStoreError};
use crate::v2::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::v2::workers::base::{Worker, WorkerError, WorkerHandle};
use crate::v2::workers::pool::TableSyncWorkerPool;
use crate::v2::workers::table_sync::{TableSyncWorker, TableSyncWorkerStateError};
use postgres::schema::Oid;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;
use tracing::{error, info};

#[derive(Debug, Error)]
pub enum ApplyWorkerError {
    #[error("An error occurred while interacting with the pipeline state store: {0}")]
    PipelineStateStoreError(#[from] PipelineStateStoreError),

    #[error("An error occurred in the apply loop: {0}")]
    ApplyLoop(#[from] ApplyLoopError),
}

#[derive(Debug, Error)]
pub enum ApplyWorkerHookError {
    #[error("An error occurred while interacting with the pipeline state store: {0}")]
    PipelineStateStoreError(#[from] PipelineStateStoreError),

    #[error("An error occurred while interacting with the table sync worker state: {0}")]
    TableSyncWorkerState(#[from] TableSyncWorkerStateError),
}

#[derive(Debug)]
pub struct ApplyWorkerHandle {
    handle: Option<JoinHandle<Result<(), ApplyWorkerError>>>,
}

impl WorkerHandle<()> for ApplyWorkerHandle {
    fn state(&self) {}

    async fn wait(mut self) -> Result<(), WorkerError> {
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
    replication_client: PgReplicationClient,
    pool: TableSyncWorkerPool,
    state_store: S,
    destination: D,
}

impl<S, D> ApplyWorker<S, D> {
    pub fn new(
        identity: PipelineIdentity,
        replication_client: PgReplicationClient,
        pool: TableSyncWorkerPool,
        state_store: S,
        destination: D,
    ) -> Self {
        Self {
            identity,
            replication_client,
            pool,
            state_store,
            destination,
        }
    }
}

impl<S, D> Worker<ApplyWorkerHandle, ()> for ApplyWorker<S, D>
where
    S: PipelineStateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    async fn start(self) -> Result<ApplyWorkerHandle, WorkerError> {
        info!("Starting apply worker");

        // We load the initial state that will be used for the apply worker.
        let pipeline_state = self
            .state_store
            .load_pipeline_state(&self.identity.id())
            .await
            .map_err(ApplyWorkerError::PipelineStateStoreError)?;

        let apply_worker = async move {
            // We start the applying loop by starting from the last LSN that we know was applied
            // by the destination.
            let hook = Hook::new(
                self.identity,
                self.replication_client.clone(),
                self.pool,
                self.state_store.clone(),
                self.destination.clone(),
            );
            start_apply_loop(
                hook,
                self.replication_client,
                pipeline_state.last_lsn,
                self.state_store,
                self.destination,
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
    replication_client: PgReplicationClient,
    pool: TableSyncWorkerPool,
    state_store: S,
    destination: D,
}

impl<S, D> Hook<S, D> {
    fn new(
        identity: PipelineIdentity,
        replication_client: PgReplicationClient,
        pool: TableSyncWorkerPool,
        state_store: S,
        destination: D,
    ) -> Self {
        Self {
            identity,
            replication_client,
            pool,
            state_store,
            destination,
        }
    }
}

impl<S, D> ApplyLoopHook for Hook<S, D>
where
    S: PipelineStateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    type Error = ApplyWorkerHookError;

    async fn process_syncing_tables(&self, current_lsn: PgLsn) -> Result<(), Self::Error> {
        let table_replication_states = self.state_store.load_table_replication_states().await?;
        info!(
            "Processing syncing tables for apply worker with LSN {}",
            current_lsn
        );

        for table_replication_state in table_replication_states {
            if let TableReplicationPhase::SyncDone { lsn } = table_replication_state.phase {
                if current_lsn >= lsn {
                    let table_replication_state = table_replication_state
                        .with_phase(TableReplicationPhase::Ready { lsn: current_lsn });
                    self.state_store
                        .store_table_replication_state(table_replication_state, true)
                        .await?;
                }
            } else {
                {
                    let pool = self.pool.read().await;
                    if let Some(table_sync_worker_state) =
                        pool.get_worker_state(table_replication_state.id)
                    {
                        let mut catchup_started = false;
                        let mut inner = table_sync_worker_state.inner().write().await;
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
                            let _ = table_sync_worker_state
                                .wait_for_phase_type(TableReplicationPhaseType::SyncDone)
                                .await;
                        }

                        continue;
                    }
                }

                info!(
                    "Creating new sync worker for table {}",
                    table_replication_state.id
                );

                match self.replication_client.duplicate().await {
                    Ok(duplicate_replication_client) => {
                        let worker = TableSyncWorker::new(
                            self.identity.clone(),
                            duplicate_replication_client,
                            self.pool.clone(),
                            table_replication_state.id,
                            self.state_store.clone(),
                            self.destination.clone(),
                        );

                        let mut table_sync_workers = self.pool.write().await;
                        table_sync_workers.start_worker(worker).await;
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

        let inner = table_sync_worker_state.inner().read().await;
        match inner.replication_phase() {
            TableReplicationPhase::Ready { .. } => true,
            TableReplicationPhase::SyncDone { lsn } => lsn <= remote_final_lsn,
            _ => false,
        }
    }
}
