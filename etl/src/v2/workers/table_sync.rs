use postgres::schema::Oid;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{watch, Notify, RwLock, RwLockReadGuard};
use tokio::task::{yield_now, JoinHandle};
use tokio_postgres::types::PgLsn;
use tracing::{info, warn};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use crate::v2::concurrency::future::ReactiveFuture;
use crate::v2::config::pipeline::PipelineConfig;
use crate::v2::destination::base::Destination;
use crate::v2::pipeline::PipelineIdentity;
use crate::v2::replication::apply::{start_apply_loop, ApplyLoopError, ApplyLoopHook};
use crate::v2::replication::client::PgReplicationClient;
use crate::v2::replication::table_sync::{start_table_sync, TableSyncError, TableSyncResult};
use crate::v2::state::store::base::{StateStore, StateStoreError};
use crate::v2::state::table::{
    TableReplicationPhase, TableReplicationPhaseType, TableReplicationState,
};
use crate::v2::workers::base::{Worker, WorkerHandle, WorkerWaitError};
use crate::v2::workers::pool::TableSyncWorkerPool;

const PHASE_CHANGE_REFRESH_FREQUENCY: Duration = Duration::from_millis(100);

#[derive(Debug, Error)]
pub enum TableSyncWorkerError {
    #[error("An error occurred while syncing a table: {0}")]
    TableSync(#[from] TableSyncError),

    #[error("The replication state is missing for table {0}")]
    ReplicationStateMissing(Oid),

    #[error("An error occurred while interacting with the pipeline state store: {0}")]
    StateStoreError(#[from] StateStoreError),

    #[error("An error occurred in the apply loop: {0}")]
    ApplyLoop(#[from] ApplyLoopError),
}

#[derive(Debug, Error)]
pub enum TableSyncWorkerHookError {}

#[derive(Debug, Error)]
pub enum TableSyncWorkerStateError {
    #[error("An error occurred while interacting with the pipeline state store: {0}")]
    StateStoreError(#[from] StateStoreError),
}

#[derive(Debug)]
pub struct TableSyncWorkerStateInner {
    table_replication_state: TableReplicationState,
    phase_change: Arc<Notify>,
}

async fn log_to_file(table_id: Oid, component: &str, message: &str) {
    let logs_dir = Path::new("./logs");
    if !logs_dir.exists() {
        let _ = fs::create_dir_all(logs_dir);
    }
    
    let file_path = format!("./logs/table_sync_worker_{}.log", table_id);
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)
    {
        let _ = writeln!(file, "[{}] {}", component, message);
    }
}

impl TableSyncWorkerStateInner {
    pub fn set_phase(&mut self, phase: TableReplicationPhase) {
        let table_id = self.table_replication_state.table_id;
        
        info!(
            "Table {} phase changing from {:?} to {:?}",
            table_id, self.table_replication_state.phase, phase
        );

        self.table_replication_state.phase = phase;
        self.phase_change.notify_waiters();
    }

    pub async fn set_phase_with<S: StateStore>(
        &mut self,
        phase: TableReplicationPhase,
        state_store: S,
    ) -> Result<(), TableSyncWorkerStateError> {
        let table_id = self.table_replication_state.table_id;
        
        self.set_phase(phase);

        if phase.as_type().should_store() {
            info!(
                "Storing phase change for table {} to {:?}",
                table_id, phase
            );

            log_to_file(
                table_id,
                "WORKER",
                &format!("Storing phase change for table {} to {:?}", table_id, phase),
            )
            .await;

            let new_table_replication_state =
                self.table_replication_state.clone().with_phase(phase);
            state_store
                .store_table_replication_state(new_table_replication_state, true)
                .await?;
        }

        Ok(())
    }

    pub fn table_id(&self) -> Oid {
        self.table_replication_state.table_id
    }

    pub fn replication_phase(&self) -> TableReplicationPhase {
        self.table_replication_state.phase
    }
}

#[derive(Debug, Clone)]
pub struct TableSyncWorkerState {
    inner: Arc<RwLock<TableSyncWorkerStateInner>>,
}

impl TableSyncWorkerState {
    fn new(table_replication_state: TableReplicationState) -> Self {
        let inner = TableSyncWorkerStateInner {
            table_replication_state,
            phase_change: Arc::new(Notify::new()),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub fn get_inner(&self) -> &RwLock<TableSyncWorkerStateInner> {
        &self.inner
    }

    pub async fn wait_for_phase_type(
        &self,
        phase_type: TableReplicationPhaseType,
    ) -> RwLockReadGuard<'_, TableSyncWorkerStateInner> {
        info!("Waiting for phase type '{:?}'", phase_type);

        loop {
            // We grab hold of the phase change notify in case we don't immediately have the state
            // that we want.
            let phase_change = {
                let inner = self.inner.read().await;
                if inner.table_replication_state.phase.as_type() == phase_type {
                    info!(
                        "Phase type '{:?}' was already set, no need to wait",
                        phase_type
                    );
                    return inner;
                }

                inner.phase_change.clone()
            };

            // We wait for a state change within a timeout. This is done since it might be that a
            // notification is missed and in that case we want to avoid blocking indefinitely.
            let _ =
                tokio::time::timeout(PHASE_CHANGE_REFRESH_FREQUENCY, phase_change.notified()).await;

            // We read the state and return the lock to the state.
            let inner = self.inner.read().await;
            if inner.table_replication_state.phase.as_type() == phase_type {
                info!("Phase type '{:?}' was noticed", phase_type);
                return inner;
            }
        }
    }
}

#[derive(Debug)]
pub struct TableSyncWorkerHandle {
    state: TableSyncWorkerState,
    handle: Option<JoinHandle<Result<(), TableSyncWorkerError>>>,
}

impl WorkerHandle<TableSyncWorkerState> for TableSyncWorkerHandle {
    fn state(&self) -> TableSyncWorkerState {
        self.state.clone()
    }

    async fn wait(mut self) -> Result<(), WorkerWaitError> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };

        handle.await??;

        Ok(())
    }
}

#[derive(Debug)]
pub struct TableSyncWorker<S, D> {
    identity: PipelineIdentity,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    pool: TableSyncWorkerPool,
    table_id: Oid,
    state_store: S,
    destination: D,
    shutdown_rx: watch::Receiver<()>,
}

impl<S, D> TableSyncWorker<S, D> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        identity: PipelineIdentity,
        config: Arc<PipelineConfig>,
        replication_client: PgReplicationClient,
        pool: TableSyncWorkerPool,
        table_id: Oid,
        state_store: S,
        destination: D,
        shutdown_rx: watch::Receiver<()>,
    ) -> Self {
        Self {
            identity,
            config,
            replication_client,
            pool,
            table_id,
            state_store,
            destination,
            shutdown_rx,
        }
    }

    pub fn table_id(&self) -> Oid {
        self.table_id
    }
}

impl<S, D> Worker<TableSyncWorkerHandle, TableSyncWorkerState> for TableSyncWorker<S, D>
where
    S: StateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    type Error = TableSyncWorkerError;

    async fn start(self) -> Result<TableSyncWorkerHandle, Self::Error> {
        info!("Starting table sync worker for table {}", self.table_id);
        
        log_to_file(
            self.table_id,
            "WORKER",
            &format!("Starting table sync worker for table {}", self.table_id),
        )
        .await;

        let Some(relation_subscription_state) = self
            .state_store
            .load_table_replication_state(self.identity.id(), self.table_id)
            .await
            .map_err(TableSyncWorkerError::StateStoreError)?
        else {
            warn!(
                "No replication state found for table {}, cannot start sync worker",
                self.table_id
            );
            
            log_to_file(
                self.table_id,
                "WORKER",
                &format!(
                    "ERROR: No replication state found for table {}, cannot start sync worker",
                    self.table_id
                ),
            )
            .await;
            
            return Err(TableSyncWorkerError::ReplicationStateMissing(self.table_id));
        };

        let state = TableSyncWorkerState::new(relation_subscription_state);

        let state_clone = state.clone();
        let table_sync_worker = async move {
            let result = start_table_sync(
                self.identity.clone(),
                self.config.clone(),
                self.replication_client.clone(),
                state_clone,
                self.state_store.clone(),
                self.destination.clone(),
                self.shutdown_rx.clone(),
            )
            .await;

            let consistent_point = match result {
                Ok(result) => {
                    match result {
                        TableSyncResult::SyncNotRequired => {
                            log_to_file(
                                self.table_id,
                                "WORKER",
                                &format!("Sync not required for table {}", self.table_id),
                            )
                            .await;
                            return Ok(());
                        }
                        TableSyncResult::SyncCompleted {
                            origin_start_lsn: consistent_point,
                        } => {
                            log_to_file(
                                self.table_id,
                                "WORKER",
                                &format!(
                                    "Sync completed for table {} with consistent point {}",
                                    self.table_id, consistent_point
                                ),
                            )
                            .await;
                            consistent_point
                        }
                    }
                }
                Err(err) => {
                    log_to_file(
                        self.table_id,
                        "WORKER",
                        &format!("ERROR: Sync failed for table {}: {}", self.table_id, err),
                    )
                    .await;
                    return Err(err.into());
                }
            };

            // let hook = Hook::new(self.table_id);
            // start_apply_loop(
            //     hook,
            //     self.config,
            //     self.replication_client,
            //     consistent_point,
            //     self.state_store,
            //     self.destination,
            //     self.shutdown_rx,
            // )
            // .await?;

            Ok(())
        };

        // let handle = tokio::spawn(ReactiveFuture::new(
        //     table_sync_worker,
        //     self.table_id,
        //     self.pool.workers(),
        // ));
        
        let handle = tokio::spawn(table_sync_worker);

        Ok(TableSyncWorkerHandle {
            state,
            handle: Some(handle),
        })
    }
}

#[derive(Debug)]
struct Hook {
    table_id: Oid,
}

impl Hook {
    fn new(table_id: Oid) -> Self {
        Self { table_id }
    }
}

impl ApplyLoopHook for Hook {
    type Error = TableSyncWorkerHookError;

    async fn process_syncing_tables(&self, current_lsn: PgLsn) -> Result<(), Self::Error> {
        info!(
            "Processing syncing tables for table sync worker with LSN {}",
            current_lsn
        );
        yield_now().await;
        // TODO: implement.
        Ok(())
    }

    async fn should_apply_changes(&self, table_id: Oid, _remote_final_lsn: PgLsn) -> bool {
        let should_apply = self.table_id == table_id;
        if should_apply {
            info!(
                "Table sync worker for table {} will apply changes",
                table_id
            );
        }

        should_apply
    }
}
