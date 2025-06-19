use crate::v2::concurrency::shutdown::ShutdownRx;
use crate::v2::config::pipeline::PipelineConfig;
use crate::v2::destination::base::Destination;
use crate::v2::pipeline::PipelineIdentity;
use crate::v2::replication::apply::{start_apply_loop, ApplyLoopError, ApplyLoopHook};
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::replication::slot::{get_slot_name, SlotError};
use crate::v2::schema::cache::SchemaCache;
use crate::v2::state::store::base::{StateStore, StateStoreError};
use crate::v2::state::table::{
    TableReplicationPhase, TableReplicationPhaseType, TableReplicationState,
};
use crate::v2::workers::base::{Worker, WorkerHandle, WorkerType, WorkerWaitError};
use crate::v2::workers::pool::TableSyncWorkerPool;
use crate::v2::workers::table_sync::{
    TableSyncWorker, TableSyncWorkerError, TableSyncWorkerState, TableSyncWorkerStateError,
};
use postgres::schema::Oid;
use std::sync::Arc;
use thiserror::Error;
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
    schema_cache: SchemaCache,
    state_store: S,
    destination: D,
    shutdown_rx: ShutdownRx,
}

impl<S, D> ApplyWorker<S, D> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: PipelineIdentity,
        config: Arc<PipelineConfig>,
        replication_client: PgReplicationClient,
        pool: TableSyncWorkerPool,
        schema_cache: SchemaCache,
        state_store: S,
        destination: D,
        shutdown_rx: ShutdownRx,
    ) -> Self {
        Self {
            identity,
            config,
            replication_client,
            pool,
            schema_cache,
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
            let start_lsn = initialize_apply_loop(&self.identity, &self.replication_client).await?;

            start_apply_loop(
                self.identity.clone(),
                start_lsn,
                self.config.clone(),
                self.replication_client.clone(),
                self.schema_cache.clone(),
                self.destination.clone(),
                Hook::new(
                    self.identity,
                    self.config,
                    self.replication_client,
                    self.pool,
                    self.schema_cache,
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

async fn initialize_apply_loop(
    identity: &PipelineIdentity,
    replication_client: &PgReplicationClient,
) -> Result<PgLsn, ApplyWorkerError> {
    let slot_name = get_slot_name(identity, WorkerType::Apply)?;
    let slot = replication_client.get_or_create_slot(&slot_name).await?;
    Ok(slot.get_start_lsn())
}

#[derive(Debug)]
struct Hook<S, D> {
    identity: PipelineIdentity,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    pool: TableSyncWorkerPool,
    schema_cache: SchemaCache,
    state_store: S,
    destination: D,
    shutdown_rx: ShutdownRx,
}

impl<S, D> Hook<S, D> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        identity: PipelineIdentity,
        config: Arc<PipelineConfig>,
        replication_client: PgReplicationClient,
        pool: TableSyncWorkerPool,
        schema_cache: SchemaCache,
        state_store: S,
        destination: D,
        shutdown_rx: ShutdownRx,
    ) -> Self {
        Self {
            identity,
            config,
            replication_client,
            pool,
            schema_cache,
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
            self.schema_cache.clone(),
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
    ) -> Result<bool, ApplyWorkerHookError> {
        let table_sync_worker_state = {
            let pool = self.pool.read().await;
            pool.get_active_worker_state(table_id)
        };

        let Some(table_sync_worker_state) = table_sync_worker_state else {
            info!("Creating new sync worker for table {}", table_id);
            self.start_table_sync_worker(table_id).await?;

            return Ok(true);
        };

        self.handle_existing_worker(table_id, table_sync_worker_state, current_lsn)
            .await
    }

    async fn handle_existing_worker(
        &self,
        table_id: Oid,
        table_sync_worker_state: TableSyncWorkerState,
        current_lsn: PgLsn,
    ) -> Result<bool, ApplyWorkerHookError> {
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
            let result = table_sync_worker_state
                .wait_for_phase_type(
                    TableReplicationPhaseType::SyncDone,
                    self.shutdown_rx.clone(),
                )
                .await;

            // If we are told to shut down while waiting for a phase change, we will signal this to
            // the caller.
            if result.should_shutdown() {
                return Ok(false);
            }

            info!("Sync completed for table {}", table_id);
        }

        Ok(true)
    }

    async fn active_table_replication_states(
        &self,
    ) -> Result<Vec<TableReplicationState>, ApplyWorkerHookError> {
        let mut table_replication_states = self.state_store.load_table_replication_states().await?;
        table_replication_states.retain(|s| !s.phase.as_type().is_done());

        Ok(table_replication_states)
    }
}

impl<S, D> ApplyLoopHook for Hook<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    type Error = ApplyWorkerHookError;

    async fn initialize(&self) -> Result<(), Self::Error> {
        let table_replication_states = self.active_table_replication_states().await?;

        for table_replication_state in table_replication_states {
            let table_id = table_replication_state.table_id;

            let table_sync_worker_state = {
                let pool = self.pool.read().await;
                pool.get_active_worker_state(table_id)
            };

            if table_sync_worker_state.is_none() {
                if let Err(err) = self.start_table_sync_worker(table_id).await {
                    error!("Error handling syncing table {}: {}", table_id, err);
                }
            }
        }

        Ok(())
    }

    async fn process_syncing_tables(&self, current_lsn: PgLsn) -> Result<bool, Self::Error> {
        let table_replication_states = self.active_table_replication_states().await?;
        info!(
            "Processing syncing tables for apply worker with LSN {}",
            current_lsn
        );

        for table_replication_state in table_replication_states {
            // We read the state store state first, if we don't find `SyncDone` we will attempt to
            // read the shared state which can contain also non-persisted states.
            let table_id = table_replication_state.table_id;
            match table_replication_state.phase {
                TableReplicationPhase::SyncDone { lsn } if current_lsn >= lsn => {
                    let updated_state = table_replication_state
                        .with_phase(TableReplicationPhase::Ready { lsn: current_lsn });
                    self.state_store
                        .store_table_replication_state(updated_state)
                        .await?;
                }
                _ => {
                    if let Err(err) = self.handle_syncing_table(table_id, current_lsn).await {
                        error!("Error handling syncing table {}: {}", table_id, err);
                    }
                }
            }
        }

        Ok(true)
    }

    async fn skip_table(&self, table_id: Oid) -> Result<bool, Self::Error> {
        let table_sync_worker_state = {
            let pool = self.pool.read().await;
            pool.get_active_worker_state(table_id)
        };

        // In case we have the state in memory, we will also update that.
        if let Some(table_sync_worker_state) = table_sync_worker_state {
            let mut inner = table_sync_worker_state.get_inner().write().await;
            inner.set_phase(TableReplicationPhase::Skipped);
        }

        // We store the new skipped state in the state store, since we want to still skip a table in
        // case of pipeline restarts.
        let table_replication_state = TableReplicationState::new(
            self.identity.id(),
            table_id,
            TableReplicationPhase::Skipped,
        );
        self.state_store
            .store_table_replication_state(table_replication_state)
            .await?;

        Ok(true)
    }

    async fn should_apply_changes(
        &self,
        table_id: Oid,
        remote_final_lsn: PgLsn,
    ) -> Result<bool, Self::Error> {
        let pool = self.pool.read().await;

        // We try to load the state first from memory, if we don't find it, we try to load from the
        // state store.
        let replication_phase = match pool.get_active_worker_state(table_id) {
            Some(state) => {
                let inner = state.get_inner().read().await;
                inner.replication_phase()
            }
            None => {
                let Some(state) = self
                    .state_store
                    .load_table_replication_state(self.identity.id(), table_id)
                    .await?
                else {
                    // If we don't even find the state for this table, we skip the event entirely.
                    return Ok(false);
                };

                state.phase
            }
        };

        let should_apply_changes = match replication_phase {
            TableReplicationPhase::Ready { .. } => true,
            TableReplicationPhase::SyncDone { lsn } => lsn <= remote_final_lsn,
            _ => false,
        };

        Ok(should_apply_changes)
    }

    fn worker_type(&self) -> WorkerType {
        WorkerType::Apply
    }
}
