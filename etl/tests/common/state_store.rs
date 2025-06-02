use etl::v2::pipeline::PipelineId;
use etl::v2::state::pipeline::PipelineState;
use etl::v2::state::store::base::{StateStore, StateStoreError};
use etl::v2::state::table::{TableReplicationPhaseType, TableReplicationState};
use postgres::schema::{Oid, TableSchema};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{Notify, RwLock};

type TableStateCondition = Box<dyn Fn(&TableReplicationState) -> bool + Send + Sync>;

struct Inner {
    pipeline_states: HashMap<PipelineId, PipelineState>,
    table_replication_states: HashMap<(PipelineId, Oid), TableReplicationState>,
    table_schemas: HashMap<(PipelineId, Oid), TableSchema>,
    table_state_conditions: Vec<((PipelineId, Oid), TableStateCondition, Arc<Notify>)>,
}

#[derive(Clone)]
pub struct TestStateStore {
    inner: Arc<RwLock<Inner>>,
}

impl TestStateStore {
    pub fn new() -> Self {
        let inner = Inner {
            pipeline_states: HashMap::new(),
            table_replication_states: HashMap::new(),
            table_schemas: HashMap::new(),
            table_state_conditions: Vec::new(),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub async fn get_pipeline_states(&self) -> HashMap<PipelineId, PipelineState> {
        let inner = self.inner.read().await;
        inner.pipeline_states.clone()
    }

    pub async fn get_table_replication_states(
        &self,
    ) -> HashMap<(PipelineId, Oid), TableReplicationState> {
        let inner = self.inner.read().await;
        inner.table_replication_states.clone()
    }

    pub async fn get_table_schemas(&self) -> HashMap<(PipelineId, Oid), TableSchema> {
        let inner = self.inner.read().await;
        inner.table_schemas.clone()
    }

    pub async fn notify_on_replication_state<F>(
        &self,
        pipeline_id: PipelineId,
        table_id: Oid,
        condition: F,
    ) -> Arc<Notify>
    where
        F: Fn(&TableReplicationState) -> bool + Send + Sync + 'static,
    {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner.table_state_conditions.push((
            (pipeline_id, table_id),
            Box::new(condition),
            notify.clone(),
        ));
        notify
    }

    pub async fn notify_on_replication_phase(
        &self,
        pipeline_id: PipelineId,
        table_id: Oid,
        phase_type: TableReplicationPhaseType,
    ) -> Arc<Notify> {
        self.notify_on_replication_state(pipeline_id, table_id, move |state| {
            state.phase.as_type() == phase_type
        })
        .await
    }

    pub async fn refresh(&self) {
        self.check_conditions().await;
    }

    async fn check_conditions(&self) {
        let mut inner = self.inner.write().await;

        // Check table state conditions
        let table_states = inner.table_replication_states.clone();
        inner
            .table_state_conditions
            .retain(|((pid, tid), condition, notify)| {
                if let Some(state) = table_states.get(&(*pid, *tid)) {
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
}

impl Default for TestStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for TestStateStore {
    async fn load_pipeline_state(
        &self,
        pipeline_id: PipelineId,
    ) -> Result<PipelineState, StateStoreError> {
        let inner = self.inner.read().await;
        inner
            .pipeline_states
            .get(&pipeline_id)
            .cloned()
            .ok_or(StateStoreError::PipelineStateNotFound)
    }

    async fn store_pipeline_state(
        &self,
        state: PipelineState,
        overwrite: bool,
    ) -> Result<bool, StateStoreError> {
        let mut inner = self.inner.write().await;
        let pipeline_id = state.id.clone();

        if !overwrite && inner.pipeline_states.contains_key(&pipeline_id) {
            return Ok(false);
        }

        inner.pipeline_states.insert(pipeline_id, state);

        Ok(true)
    }

    async fn load_table_replication_state(
        &self,
        pipeline_id: PipelineId,
        table_id: Oid,
    ) -> Result<Option<TableReplicationState>, StateStoreError> {
        let inner = self.inner.read().await;

        Ok(inner
            .table_replication_states
            .get(&(pipeline_id, table_id))
            .cloned())
    }

    async fn load_table_replication_states(
        &self,
    ) -> Result<Vec<TableReplicationState>, StateStoreError> {
        let inner = self.inner.read().await;

        Ok(inner.table_replication_states.values().cloned().collect())
    }

    async fn store_table_replication_state(
        &self,
        pipeline_id: PipelineId,
        state: TableReplicationState,
        overwrite: bool,
    ) -> Result<bool, StateStoreError> {
        let mut inner = self.inner.write().await;
        let key = (pipeline_id, state.id);

        if !overwrite && inner.table_replication_states.contains_key(&key) {
            return Ok(false);
        }

        inner.table_replication_states.insert(key, state);
        drop(inner); // Release the write lock before checking conditions

        self.check_conditions().await;

        Ok(true)
    }

    async fn load_table_schemas(
        &self,
        pipeline_id: PipelineId,
    ) -> Result<Vec<TableSchema>, StateStoreError> {
        let inner = self.inner.read().await;
        Ok(inner
            .table_schemas
            .iter()
            .filter(|((pid, _), _)| pid == &pipeline_id)
            .map(|(_, schema)| schema.clone())
            .collect())
    }

    async fn load_table_schema(
        &self,
        pipeline_id: PipelineId,
        table_id: Oid,
    ) -> Result<Option<TableSchema>, StateStoreError> {
        let inner = self.inner.read().await;

        Ok(inner.table_schemas.get(&(pipeline_id, table_id)).cloned())
    }

    async fn store_table_schema(
        &self,
        pipeline_id: PipelineId,
        table_schema: TableSchema,
        overwrite: bool,
    ) -> Result<bool, StateStoreError> {
        let mut inner = self.inner.write().await;
        let key = (pipeline_id, table_schema.id);

        if !overwrite && inner.table_schemas.contains_key(&key) {
            return Ok(false);
        }

        inner.table_schemas.insert(key, table_schema);

        Ok(true)
    }
}

impl fmt::Debug for TestStateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = tokio::task::block_in_place(move || {
            Handle::current().block_on(async move { self.inner.read().await })
        });
        f.debug_struct("TestStateStore")
            .field("pipeline_states", &inner.pipeline_states)
            .field("table_replication_states", &inner.table_replication_states)
            .field("table_schemas", &inner.table_schemas)
            .finish()
    }
}
