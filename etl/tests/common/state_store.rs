use etl::v2::state::store::base::{StateStore, StateStoreError};
use etl::v2::state::table::{TableReplicationPhaseType, TableReplicationState};
use postgres::schema::{Oid, TableId};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{Notify, RwLock};

type TableStateCondition = Box<dyn Fn(&TableReplicationState) -> bool + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StateStoreMethod {
    LoadTableReplicationState,
    LoadTableReplicationStates,
    StoreTableReplicationState,
}

struct Inner {
    table_replication_states: HashMap<TableId, TableReplicationState>,
    table_state_conditions: Vec<(TableId, TableStateCondition, Arc<Notify>)>,
    method_call_notifiers: HashMap<StateStoreMethod, Vec<Arc<Notify>>>,
}

impl Inner {
    async fn check_conditions(&mut self) {
        let table_states = self.table_replication_states.clone();
        self.table_state_conditions
            .retain(|(tid, condition, notify)| {
                if let Some(state) = table_states.get(tid) {
                    let should_retain = !condition(state);
                    if !should_retain {
                        notify.notify_one();
                    }
                    should_retain
                } else {
                    true
                }
            });
    }

    async fn dispatch_method_notification(&self, method: StateStoreMethod) {
        if let Some(notifiers) = self.method_call_notifiers.get(&method) {
            for notifier in notifiers {
                notifier.notify_one();
            }
        }
    }
}

#[derive(Clone)]
pub struct TestStateStore {
    inner: Arc<RwLock<Inner>>,
}

impl TestStateStore {
    pub fn new() -> Self {
        let inner = Inner {
            table_replication_states: HashMap::new(),
            table_state_conditions: Vec::new(),
            method_call_notifiers: HashMap::new(),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub async fn get_table_replication_states(&self) -> HashMap<TableId, TableReplicationState> {
        let inner = self.inner.read().await;
        inner.table_replication_states.clone()
    }

    pub async fn notify_on_replication_state<F>(&self, table_id: Oid, condition: F) -> Arc<Notify>
    where
        F: Fn(&TableReplicationState) -> bool + Send + Sync + 'static,
    {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner
            .table_state_conditions
            .push((table_id, Box::new(condition), notify.clone()));

        notify
    }

    pub async fn notify_on_replication_phase(
        &self,
        table_id: Oid,
        phase_type: TableReplicationPhaseType,
    ) -> Arc<Notify> {
        self.notify_on_replication_state(table_id, move |state| state.phase.as_type() == phase_type)
            .await
    }
}

impl StateStore for TestStateStore {
    async fn load_table_replication_state(
        &self,
        table_id: Oid,
    ) -> Result<Option<TableReplicationState>, StateStoreError> {
        let inner = self.inner.read().await;
        let result = Ok(inner.table_replication_states.get(&table_id).cloned());

        inner
            .dispatch_method_notification(StateStoreMethod::LoadTableReplicationState)
            .await;

        result
    }

    async fn load_table_replication_states(
        &self,
    ) -> Result<Vec<TableReplicationState>, StateStoreError> {
        let inner = self.inner.read().await;
        let result = Ok(inner.table_replication_states.values().cloned().collect());

        inner
            .dispatch_method_notification(StateStoreMethod::LoadTableReplicationStates)
            .await;

        result
    }

    async fn store_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationState,
    ) -> Result<(), StateStoreError> {
        let mut inner = self.inner.write().await;
        inner.table_replication_states.insert(table_id, state);
        inner.check_conditions().await;
        inner
            .dispatch_method_notification(StateStoreMethod::StoreTableReplicationState)
            .await;
        Ok(())
    }
}

impl fmt::Debug for TestStateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = tokio::task::block_in_place(move || {
            Handle::current().block_on(async move { self.inner.read().await })
        });
        f.debug_struct("TestStateStore")
            .field("table_replication_states", &inner.table_replication_states)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub enum FaultType {
    Panic,
    // Commenting out to fix clippy error because this is unused, comment in when this starts being used
    // Error,
}

#[derive(Debug, Clone, Default)]
pub struct FaultConfig {
    pub load_table_replication_state: Option<FaultType>,
    pub load_table_replication_states: Option<FaultType>,
    pub store_table_replication_state: Option<FaultType>,
}

#[derive(Debug, Clone)]
pub struct FaultInjectingStateStore<S>
where
    S: Clone,
{
    inner: S,
    config: Arc<FaultConfig>,
}

impl<S> FaultInjectingStateStore<S>
where
    S: Clone,
{
    pub fn wrap(inner: S, config: FaultConfig) -> Self {
        Self {
            inner,
            config: Arc::new(config),
        }
    }

    pub fn get_inner(&self) -> &S {
        &self.inner
    }

    fn trigger_fault(&self, fault: &Option<FaultType>) -> Result<(), StateStoreError> {
        if let Some(fault_type) = fault {
            match fault_type {
                FaultType::Panic => panic!("Fault injection: panic triggered"),
                // Commenting out to fix clippy error because this is unused, comment in when this starts being used
                // // We trigger a random error.
                // FaultType::Error => return Err(StateStoreError::TableReplicationStateNotFound),
            }
        }

        Ok(())
    }
}

impl<S> StateStore for FaultInjectingStateStore<S>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    async fn load_table_replication_state(
        &self,
        table_id: Oid,
    ) -> Result<Option<TableReplicationState>, StateStoreError> {
        self.trigger_fault(&self.config.load_table_replication_state)?;
        self.inner.load_table_replication_state(table_id).await
    }

    async fn load_table_replication_states(
        &self,
    ) -> Result<Vec<TableReplicationState>, StateStoreError> {
        self.trigger_fault(&self.config.load_table_replication_states)?;
        self.inner.load_table_replication_states().await
    }

    async fn store_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationState,
    ) -> Result<(), StateStoreError> {
        self.trigger_fault(&self.config.store_table_replication_state)?;
        self.inner
            .store_table_replication_state(table_id, state)
            .await
    }
}
