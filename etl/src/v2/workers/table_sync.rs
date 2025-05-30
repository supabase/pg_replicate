use crate::v2::destination::base::Destination;
use crate::v2::replication::apply::{start_apply_loop, ApplyLoopHook};
use crate::v2::replication::table_sync::start_table_sync;
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::state::table::{
    TableReplicationPhase, TableReplicationPhaseType, TableReplicationState,
};
use crate::v2::workers::base::{Worker, WorkerError, WorkerHandle};
use crate::v2::workers::pool::TableSyncWorkerPool;

use crate::v2::concurrency::future::ReactiveFuture;
use crate::v2::replication::client::PgReplicationClient;
use postgres::schema::Oid;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock, RwLockReadGuard};
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;
use tracing::{info, warn};

const PHASE_CHANGE_REFRESH_FREQUENCY: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub struct TableSyncWorkerStateInner {
    table_replication_state: TableReplicationState,
    phase_change: Arc<Notify>,
}

impl TableSyncWorkerStateInner {
    fn set_phase(&mut self, phase: TableReplicationPhase) {
        info!(
            "Table {} phase changing from {:?} to {:?}",
            self.table_replication_state.id, self.table_replication_state.phase, phase
        );

        self.table_replication_state.phase = phase;
        // We want to notify all waiters that there was a phase change.
        //
        // Note that this notify will not wake up waiters that will be coming in the future since
        // no permit is stored, only active listeners will be notified.
        self.phase_change.notify_waiters();
    }

    pub async fn set_phase_with<S: PipelineStateStore>(
        &mut self,
        phase: TableReplicationPhase,
        state_store: S,
    ) {
        self.set_phase(phase);

        // If we should store this phase change, we want to do it via the supplied state store.
        if phase.as_type().should_store() {
            info!(
                "Storing phase change for table {} to {:?}",
                self.table_replication_state.id, phase
            );

            let new_table_replication_state =
                self.table_replication_state.clone().with_phase(phase);
            state_store
                .store_table_replication_state(new_table_replication_state)
                .await;
        }
    }

    pub fn phase(&self) -> TableReplicationPhase {
        self.table_replication_state.phase
    }
}

#[derive(Debug, Clone)]
pub struct TableSyncWorkerState {
    inner: Arc<RwLock<TableSyncWorkerStateInner>>,
}

impl TableSyncWorkerState {
    fn new(relation_subscription_state: TableReplicationState) -> Self {
        let inner = TableSyncWorkerStateInner {
            table_replication_state: relation_subscription_state,
            phase_change: Arc::new(Notify::new()),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub fn inner(&self) -> &RwLock<TableSyncWorkerStateInner> {
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
    handle: Option<JoinHandle<()>>,
}

impl WorkerHandle<TableSyncWorkerState> for TableSyncWorkerHandle {
    fn state(&self) -> TableSyncWorkerState {
        self.state.clone()
    }

    async fn wait(mut self) -> Result<(), WorkerError> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };

        handle.await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct TableSyncWorker<S, D> {
    replication_client: PgReplicationClient,
    pool: TableSyncWorkerPool,
    table_id: Oid,
    state_store: S,
    destination: D,
}

impl<S, D> TableSyncWorker<S, D> {
    pub fn new(
        replication_client: PgReplicationClient,
        pool: TableSyncWorkerPool,
        table_id: Oid,
        state_store: S,
        destination: D,
    ) -> Self {
        Self {
            replication_client,
            pool,
            table_id,
            state_store,
            destination,
        }
    }

    pub fn table_id(&self) -> Oid {
        self.table_id
    }
}

impl<S, D> Worker<TableSyncWorkerHandle, TableSyncWorkerState> for TableSyncWorker<S, D>
where
    S: PipelineStateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    async fn start(self) -> Option<TableSyncWorkerHandle> {
        info!("Starting table sync worker for table {}", self.table_id);

        let Some(relation_subscription_state) = self
            .state_store
            .load_table_replication_state(&self.table_id)
            .await
        else {
            warn!(
                "No replication state found for table {}, cannot start sync worker",
                self.table_id
            );
            return None;
        };

        let state = TableSyncWorkerState::new(relation_subscription_state);

        let state_clone = state.clone();
        let table_sync_worker = async move {
            // We first start syncing the table.
            start_table_sync(
                self.state_store.clone(),
                self.destination.clone(),
                state_clone,
            )
            .await;

            // If we succeed syncing the table, we want to start the same apply loop as in the apply
            // worker but starting from the `0/0` LSN which means that the slot is starting streaming
            // from its consistent snapshot.
            // TODO: check if this is the right LSN to start with, maybe we want the consistent
            //  point of the slot.
            let hook = Hook::new(self.table_id);
            start_apply_loop(
                hook,
                self.replication_client,
                PgLsn::from(0),
                self.state_store,
                self.destination,
            )
            .await;
        };

        // We spawn the table sync worker with a safe future, so that we can have controlled teardown
        // on completion or error.
        let handle = tokio::spawn(ReactiveFuture::new(
            table_sync_worker,
            self.table_id,
            self.pool.workers(),
        ));

        Some(TableSyncWorkerHandle {
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
    async fn process_syncing_tables(&self, current_lsn: PgLsn) {
        info!(
            "Processing syncing tables for table sync worker with LSN {}",
            current_lsn
        );
        // TODO: implement.
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
