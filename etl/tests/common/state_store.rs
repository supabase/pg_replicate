use etl::v2::pipeline::PipelineId;
use etl::v2::state::pipeline::PipelineState;
use etl::v2::state::store::base::{StateStore, StateStoreError};
use etl::v2::state::table::TableReplicationState;
use postgres::schema::{Oid, TableSchema};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
struct Inner {
    pipeline_states: HashMap<PipelineId, PipelineState>,
    table_replication_states: HashMap<(PipelineId, Oid), TableReplicationState>,
    table_schemas: HashMap<(PipelineId, Oid), TableSchema>,
}

#[derive(Debug, Clone)]
pub struct TestStateStore {
    inner: Arc<RwLock<Inner>>,
}

impl TestStateStore {
    pub fn new() -> Self {
        let inner = Inner {
            pipeline_states: HashMap::new(),
            table_replication_states: HashMap::new(),
            table_schemas: HashMap::new(),
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
