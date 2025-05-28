use crate::v2::destination::base::Destination;
use crate::v2::replication::apply::{start_apply_loop, ApplyLoopHook};
use crate::v2::replication::table_sync::start_table_sync;
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::state::table::{
    TableReplicationPhase, TableReplicationPhaseType, TableReplicationState,
};
use crate::v2::workers::base::{CatchFuture, Worker, WorkerHandle};
use crate::v2::workers::pool::TableSyncWorkerPool;
use tracing::{info, warn};

use postgres::schema::Oid;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock, RwLockReadGuard};
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;

const PHASE_CHANGE_REFRESH_FREQUENCY: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub struct TableSyncWorkerStateInner {
    table_replication_state: TableReplicationState,
    phase_change: Arc<Notify>,
}

impl TableSyncWorkerStateInner {
    pub fn set_phase(&mut self, phase: TableReplicationPhase) {
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

    // TODO: find a better API for this.
    pub fn inner(&self) -> &RwLock<TableSyncWorkerStateInner> {
        &self.inner
    }

    // TODO: check how we can design the system to actually return either a write or read lock.
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

    async fn wait(mut self) {
        // TODO: figure out a way to mark a state as invalid if the worker crashed or it
        //  was stopped since via reference counting we are blind on this.
        let Some(handle) = self.handle.take() else {
            return;
        };

        // TODO: properly handle failure.
        handle.await.expect("Table sync worker failed");
    }
}

#[derive(Debug)]
pub struct TableSyncWorker<S, D> {
    state_store: S,
    destination: D,
    table_id: Oid,
    pool: TableSyncWorkerPool,
}

impl<S, D> TableSyncWorker<S, D> {
    pub fn new(state_store: S, destination: D, table_id: Oid, pool: TableSyncWorkerPool) -> Self {
        Self {
            state_store,
            destination,
            table_id,
            pool,
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
            start_apply_loop(
                self.state_store,
                self.destination,
                Hook::new(self.table_id),
                PgLsn::from(0),
            )
            .await;
        };
        let pool = self.pool.clone();
        let table_id = self.table_id;
        let table_sync_worker = CatchFuture::new(table_sync_worker, move || {
            let pool = pool.clone();
            async move {
                info!(
                    "Table sync worker for table {} failed, removing from pool",
                    table_id
                );
                let mut pool = pool.write().await;
                pool.remove_worker(table_id).await;
            }
        });

        let handle = tokio::spawn(table_sync_worker);

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

impl<S, D> ApplyLoopHook<S, D> for Hook
where
    S: PipelineStateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    async fn process_syncing_tables(
        &self,
        state_store: S,
        destination: D,
        current_lsn: PgLsn,
    ) -> () {
        info!(
            "Processing syncing tables for table sync worker with LSN {}",
            current_lsn
        );

        // This is intentionally empty as table sync workers don't need to process other tables
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
