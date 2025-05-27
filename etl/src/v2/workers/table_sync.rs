use postgres::schema::Oid;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::task::JoinHandle;

use crate::v2::state::relation_subscription::{
    RelationSubscriptionState, RelationSubscriptionStatus,
};
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::workers::base::{Worker, WorkerHandle};

pub type TableSyncWorkerHandles = Arc<RwLock<HashMap<Oid, TableSyncWorkerHandle>>>;

#[derive(Debug)]
pub struct StateInner {
    relation_subscription_state: RelationSubscriptionState,
    state_change: Arc<Notify>,
}

impl StateInner {
    pub fn set_status(&mut self, status: RelationSubscriptionStatus) {
        self.relation_subscription_state.set_status(status);
        self.state_change.notify_waiters();
    }
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

    pub async fn wait_for_status(
        &self,
        status: RelationSubscriptionStatus,
    ) -> RwLockReadGuard<'_, StateInner> {
        loop {
            // We grab hold of the state change notify.
            let state_change = {
                let inner = self.inner.read().await;
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

    async fn wait(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };

        // TODO: properly handle message.
        handle.await.unwrap();
    }
}

#[derive(Debug)]
pub struct TableSyncWorker<S> {
    state_store: S,
    rel_id: Oid,
}

impl<S> TableSyncWorker<S>
where
    S: PipelineStateStore,
{
    pub fn new(state_store: S, rel_id: Oid) -> Self {
        Self {
            state_store,
            rel_id,
        }
    }
}

impl<S> Worker<TableSyncWorkerHandle, TableSyncWorkerState> for TableSyncWorker<S>
where
    S: PipelineStateStore + Send + 'static,
{
    async fn start(self) -> TableSyncWorkerHandle {
        let relation_subscription_states = self
            .state_store
            .load_relation_subscription_state(&self.rel_id)
            .await;

        let state = TableSyncWorkerState::new(relation_subscription_states);

        let handle = tokio::spawn(table_sync_worker(self.state_store, state.clone()));

        TableSyncWorkerHandle {
            state,
            handle: Some(handle),
        }
    }
}

async fn table_sync_worker<S>(state_store: S, state: TableSyncWorkerState)
where
    S: PipelineStateStore + Send + 'static,
{
}
