use crate::v2::destination::Destination;
use crate::v2::replication::apply::{start_apply_loop, ApplyLoopHook};
use crate::v2::replication::table_sync::start_table_sync;
use crate::v2::state::relation_subscription::{
    RelationSubscriptionState, RelationSubscriptionStatus,
};
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::workers::base::{Worker, WorkerHandle};
use postgres::schema::Oid;
use std::collections::HashMap;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::{Notify, RwLock, RwLockReadGuard, RwLockWriteGuard};
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
        S: PipelineStateStore + Send + 'static,
        D: Destination + Send + 'static,
    {
        let mut workers = self.workers.write().await;

        let rel_id = worker.rel_id;
        if workers.contains_key(&rel_id) {
            return false;
        }

        let handle = worker.start().await;
        workers.insert(rel_id, handle);

        true
    }

    pub async fn get_worker_state(&self, oid: Oid) -> Option<TableSyncWorkerState> {
        let handles = self.workers.read().await;
        Some(handles.get(&oid)?.state.clone())
    }

    pub async fn remove_worker(&self, oid: Oid) {
        let mut handles = self.workers.write().await;
        handles.remove(&oid);
    }

    pub async fn wait(&self) {
        let mut handles = self.workers.write().await;
    }
}

#[derive(Debug)]
pub struct StateInner {
    relation_subscription_state: RelationSubscriptionState,
    state_change: Arc<Notify>,
}

#[derive(Debug, Clone)]
pub struct TableSyncWorkerState {
    inner: Arc<RwLock<StateInner>>,
}

impl TableSyncWorkerState {
    fn new(relation_subscription_state: RelationSubscriptionState) -> Self {
        let inner = StateInner {
            relation_subscription_state,
            state_change: Arc::new(Notify::new()),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub async fn set_status(&self, status: RelationSubscriptionStatus) {
        let mut inner = self.inner.write().await;
        inner.relation_subscription_state.status = status;
    }

    pub async fn wait_for_status(
        &self,
        status: RelationSubscriptionStatus,
    ) -> RwLockReadGuard<'_, StateInner> {
        loop {
            // We grab hold of the state change notify in case we don't immediately have the state
            // that we want.
            let state_change = {
                let inner = self.inner.read().await;

                if inner.relation_subscription_state.status == status {
                    return inner;
                }

                inner.state_change.clone()
            };

            // We wait for a state change.
            state_change.notified().await;

            // We read the state and return the lock to the state.
            let inner = self.inner.read().await;
            if inner.relation_subscription_state.status == status {
                return inner;
            }
        }
    }
}

impl Deref for TableSyncWorkerState {
    type Target = RwLock<StateInner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
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
    rel_id: Oid,
}

impl<S, D> TableSyncWorker<S, D> {
    pub fn new(state_store: S, destination: D, rel_id: Oid) -> Self {
        Self {
            state_store,
            destination,
            rel_id,
        }
    }
}

impl<S, D> Worker<TableSyncWorkerHandle, TableSyncWorkerState> for TableSyncWorker<S, D>
where
    S: PipelineStateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    async fn start(self) -> TableSyncWorkerHandle {
        let relation_subscription_state = self
            .state_store
            .load_relation_subscription_state(&self.rel_id)
            .await;

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
                rel_id: self.rel_id,
            };
            start_apply_loop(self.state_store, self.destination, hook, PgLsn::from(0)).await;
        });

        TableSyncWorkerHandle {
            state,
            handle: Some(handle),
        }
    }
}

#[derive(Debug)]
struct Hook {
    rel_id: Oid,
}

impl<S, D> ApplyLoopHook<S, D> for Hook
where
    S: PipelineStateStore + Send + 'static,
    D: Destination + Send + 'static,
{
    async fn process_syncing_tables(
        &self,
        state_store: S,
        destination: D,
        current_lsn: PgLsn,
    ) -> () {
    }

    fn should_apply_changes(&self, rel_id: Oid) -> bool {
        self.rel_id == rel_id
    }
}
