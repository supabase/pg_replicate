use crate::v2::config::pipeline::PipelineConfig;
use crate::v2::destination::base::Destination;
use crate::v2::pipeline::PipelineIdentity;
use crate::v2::replication::apply::{start_apply_loop, ApplyLoopError, ApplyLoopHook};
use crate::v2::replication::client::{
    GetOrCreateSlotResult, PgReplicationClient, PgReplicationError,
};
use crate::v2::replication::slot::{get_slot_name, SlotError, SlotUsage};
use crate::v2::state::origin::ReplicationOriginState;
use crate::v2::state::store::base::{StateStore, StateStoreError};
use crate::v2::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::v2::workers::base::{Worker, WorkerHandle, WorkerWaitError};
use crate::v2::workers::pool::TableSyncWorkerPool;
use crate::v2::workers::table_sync::{
    TableSyncWorker, TableSyncWorkerError, TableSyncWorkerState, TableSyncWorkerStateError,
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
    StateStore(#[from] StateStoreError),

    #[error("An error occurred in the apply loop: {0}")]
    ApplyLoop(#[from] ApplyLoopError),

    #[error("A Postgres replication error occurred in the apply loop: {0}")]
    PgReplication(#[from] PgReplicationError),

    #[error("Could not generate slot name in the apply loop: {0}")]
    Slot(#[from] SlotError),

    #[error(
        "The replication origin for the apply worker was not found even though the slot was active"
    )]
    ReplicationOriginMissing,
}

#[derive(Debug, Error)]
pub enum ApplyWorkerHookError {
    #[error("An error occurred while interacting with the state store: {0}")]
    StateStore(#[from] StateStoreError),

    #[error("An error occurred while interacting with the table sync worker state: {0}")]
    TableSyncWorkerState(#[from] TableSyncWorkerStateError),

    #[error("An error occurred while trying to start the table sync worker: {0}")]
    TableSyncWorkerStartedFailed(#[from] TableSyncWorkerError),

    #[error("A Postgres replication error occurred in the apply worker: {0}")]
    PgReplication(#[from] PgReplicationError),
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
            let origin_start_lsn = initialize_apply_loop(
                &self.identity,
                &self.config,
                &self.replication_client,
                &self.state_store,
            )
            .await?;

            start_apply_loop(
                self.identity.clone(),
                origin_start_lsn,
                self.config.clone(),
                self.replication_client.clone(),
                self.state_store.clone(),
                self.destination.clone(),
                Hook::new(
                    self.identity,
                    self.config,
                    self.replication_client,
                    self.pool,
                    self.state_store,
                    self.destination,
                    self.shutdown_rx.clone(),
                ),
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

async fn initialize_apply_loop<S>(
    identity: &PipelineIdentity,
    config: &Arc<PipelineConfig>,
    replication_client: &PgReplicationClient,
    state_store: &S,
) -> Result<PgLsn, ApplyWorkerError>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    let mut attempt = 0;
    loop {
        // We get or create the slot name for the apply worker.
        let slot_name = get_slot_name(identity, SlotUsage::ApplyWorker)?;
        let slot = replication_client.get_or_create_slot(&slot_name).await?;

        // If we just created a slot, we will use its consistent point to start the apply loop,
        // otherwise we will load from the replication origin.
        let origin_start_lsn = if let GetOrCreateSlotResult::CreateSlot(slot) = slot {
            let replication_origin_state =
                ReplicationOriginState::new(identity.id(), None, slot.consistent_point);

            state_store
                .store_replication_origin_state(replication_origin_state, true)
                .await?;

            slot.consistent_point
        } else {
            let replication_origin_state = state_store
                .load_replication_origin_state(identity.id(), None)
                .await?;

            // If we didn't find any replication origin state but the slot was there, the
            // apply worker might have crashed between creating a slot and storing the
            // replication origin state. In this case, we optimistically delete the slot and
            // start from scratch.
            let Some(replication_origin_state) = replication_origin_state else {
                replication_client.delete_slot(&slot_name).await?;
                attempt += 1;

                if attempt >= config.retry_config.max_attempts {
                    return Err(ApplyWorkerError::ReplicationOriginMissing);
                }

                let delay = config.retry_config.calculate_delay(attempt);
                tokio::time::sleep(delay).await;

                continue;
            };

            replication_origin_state.remote_lsn
        };

        return Ok(origin_start_lsn);
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

impl<S, D> Hook<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    async fn start_table_sync_worker(&self, table_id: Oid) -> Result<(), ApplyWorkerHookError> {
        // TODO: switch to a connection pool which hands out connections.
        let replication_client = self.replication_client.duplicate().await?;
        let worker = TableSyncWorker::new(
            self.identity.clone(),
            self.config.clone(),
            replication_client,
            self.pool.clone(),
            table_id,
            self.state_store.clone(),
            self.destination.clone(),
            self.shutdown_rx.clone(),
        );

        let mut pool = self.pool.write().await;
        if let Err(err) = pool.start_worker(worker).await {
            // TODO: check if we want to build a backoff mechanism for retrying the
            //  spawning of new table sync workers.
            error!("Failed to start table sync worker: {}", err);

            return Err(err.into());
        }

        Ok(())
    }

    async fn handle_syncing_table(
        &self,
        table_id: Oid,
        current_lsn: PgLsn,
    ) -> Result<(), ApplyWorkerHookError> {
        let pool = self.pool.read().await;
        let table_sync_worker_state = pool.get_worker_state(table_id);
        drop(pool);

        let Some(table_sync_worker_state) = table_sync_worker_state else {
            info!("Creating new sync worker for table {}", table_id);
            self.start_table_sync_worker(table_id).await?;
            return Ok(());
        };

        self.handle_existing_worker(table_id, table_sync_worker_state, current_lsn)
            .await
    }

    async fn handle_existing_worker(
        &self,
        table_id: Oid,
        table_sync_worker_state: TableSyncWorkerState,
        current_lsn: PgLsn,
    ) -> Result<(), ApplyWorkerHookError> {
        let mut catchup_started = false;
        {
            let mut inner = table_sync_worker_state.get_inner().write().await;
            if inner.replication_phase().as_type() == TableReplicationPhaseType::SyncWait {
                inner
                    .set_phase_with(
                        TableReplicationPhase::Catchup { lsn: current_lsn },
                        self.state_store.clone(),
                    )
                    .await?;
                catchup_started = true;
            }
        }

        if catchup_started {
            info!("Waiting for sync completion for table {}", table_id);
            let _ = table_sync_worker_state
                .wait_for_phase_type(TableReplicationPhaseType::SyncDone)
                .await;
            info!("Sync completed for table {}", table_id);
        }

        Ok(())
    }
}

impl<S, D> ApplyLoopHook for Hook<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    type Error = ApplyWorkerHookError;

    async fn initialize(&self) -> Result<(), Self::Error> {
        let table_replication_states = self.state_store.load_table_replication_states().await?;

        for table_replication_state in table_replication_states {
            let table_id = table_replication_state.table_id;

            let pool = self.pool.read().await;
            let table_sync_worker_state = pool.get_worker_state(table_replication_state.table_id);
            drop(pool);

            if table_sync_worker_state.is_none() {
                if let Err(err) = self
                    .start_table_sync_worker(table_replication_state.table_id)
                    .await
                {
                    error!("Error handling syncing table {}: {}", table_id, err);
                }
            }
        }

        Ok(())
    }

    async fn process_syncing_tables(&self, current_lsn: PgLsn) -> Result<(), Self::Error> {
        let table_replication_states = self.state_store.load_table_replication_states().await?;
        info!(
            "Processing syncing tables for apply worker with LSN {}",
            current_lsn
        );

        for table_replication_state in table_replication_states {
            // We read the state store state first, if we don't find `SyncDone` we will attempt to
            // read the shared state which can contain also non-persisted states.
            match table_replication_state.phase {
                TableReplicationPhase::SyncDone { lsn } if current_lsn >= lsn => {
                    let updated_state = table_replication_state
                        .with_phase(TableReplicationPhase::Ready { lsn: current_lsn });
                    self.state_store
                        .store_table_replication_state(updated_state, true)
                        .await?;
                }
                _ => {
                    let table_id = table_replication_state.table_id;
                    if let Err(err) = self.handle_syncing_table(table_id, current_lsn).await {
                        error!("Error handling syncing table {}: {}", table_id, err);
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
