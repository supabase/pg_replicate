use crate::v2::destination::base::Destination;
use crate::v2::replication::apply::{start_apply_loop, ApplyLoopHook};
use crate::v2::replication::table_sync::start_table_sync;
use crate::v2::state::relation_subscription::{
    TableReplicationPhase, TableReplicationPhaseType, TableReplicationState,
};
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::workers::base::{Worker, WorkerHandle};

use postgres::schema::Oid;
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock, RwLockReadGuard};
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;

#[derive(Debug, Clone)]
pub struct TableSyncWorkers {
    workers: Arc<RwLock<HashMap<Oid, TableSyncWorkerHandle>>>,
}

impl TableSyncWorkers {
    pub fn new() -> TableSyncWorkers {
        Self {
            workers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start_worker<S, D>(&self, worker: TableSyncWorker<S, D>) -> bool
    where
        S: PipelineStateStore + Clone + Send + 'static,
        D: Destination + Clone + Send + 'static,
    {
        let mut workers = self.workers.write().await;

        let table_id = worker.table_id;
        if workers.contains_key(&table_id) {
            return false;
        }

        let Some(handle) = worker.start().await else {
            return false;
        };

        workers.insert(table_id, handle);

        true
    }

    pub async fn get_worker_state(&self, table_id: Oid) -> Option<TableSyncWorkerState> {
        let handles = self.workers.read().await;
        Some(handles.get(&table_id)?.state.clone())
    }

    pub async fn remove_worker(&self, table_id: Oid) {
        let mut handles = self.workers.write().await;
        handles.remove(&table_id);
    }

    pub async fn wait_all(&self) {
        let mut workers = self.workers.write().await;

        let workers = mem::take(&mut *workers);
        for (_, worker) in workers {
            worker.wait().await;
        }
    }
}

#[derive(Debug)]
pub struct StateInner {
    table_replication_state: TableReplicationState,
    phase_change: Arc<Notify>,
}

impl StateInner {
    pub fn set_phase(&mut self, phase: TableReplicationPhase) {
        self.table_replication_state.phase = phase;
    }

    pub fn get_phase(&self) -> TableReplicationPhase {
        self.table_replication_state.phase
    }
}

#[derive(Debug, Clone)]
pub struct TableSyncWorkerState {
    inner: Arc<RwLock<StateInner>>,
}

impl TableSyncWorkerState {
    fn new(relation_subscription_state: TableReplicationState) -> Self {
        let inner = StateInner {
            table_replication_state: relation_subscription_state,
            phase_change: Arc::new(Notify::new()),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub fn inner(&self) -> &RwLock<StateInner> {
        &self.inner
    }

    // TODO: check how we can design the system to actually return either a write or read lock.
    pub async fn wait_for_phase_type(
        &self,
        phase_type: TableReplicationPhaseType,
    ) -> RwLockReadGuard<'_, StateInner> {
        loop {
            // We grab hold of the state change notify in case we don't immediately have the state
            // that we want.
            let state_change = {
                let inner = self.inner.read().await;

                if inner.table_replication_state.phase.as_type() == phase_type {
                    return inner;
                }

                inner.phase_change.clone()
            };

            // We wait for a state change.
            state_change.notified().await;

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
}

impl<S, D> TableSyncWorker<S, D> {
    pub fn new(state_store: S, destination: D, table_id: Oid) -> Self {
        Self {
            state_store,
            destination,
            table_id,
        }
    }
}

impl<S, D> Worker<TableSyncWorkerHandle, TableSyncWorkerState> for TableSyncWorker<S, D>
where
    S: PipelineStateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    async fn start(self) -> Option<TableSyncWorkerHandle> {
        println!("Starting table sync worker");
        let Some(relation_subscription_state) = self
            .state_store
            .load_table_replication_state(&self.table_id)
            .await
        else {
            println!("The table doesn't exist in the store, stopping table sync worker");
            return None;
        };

        let state = TableSyncWorkerState::new(relation_subscription_state);

        let state_clone = state.clone();
        let handle = tokio::spawn(async move {
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
            // TODO: check if this is the right LSN to start with.
            let hook = Hook {
                table_id: self.table_id,
            };
            start_apply_loop(self.state_store, self.destination, hook, PgLsn::from(0)).await;
        });

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
    }

    async fn should_apply_changes(&self, table_id: Oid, _remote_final_lsn: PgLsn) -> bool {
        self.table_id == table_id
    }
}
