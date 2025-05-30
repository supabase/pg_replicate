use postgres::schema::Oid;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::v2::state::pipeline::PipelineState;
use crate::v2::state::store::base::{PipelineStateStore, PipelineStateStoreError};
use crate::v2::state::table::TableReplicationState;

#[derive(Debug)]
struct Inner {
    pipeline_state: Option<PipelineState>,
    table_replication_states: HashMap<Oid, TableReplicationState>,
}

#[derive(Debug, Clone)]
pub struct MemoryPipelineStateStore {
    inner: Arc<RwLock<Inner>>,
}

impl MemoryPipelineStateStore {
    pub fn new() -> Self {
        let inner = Inner {
            pipeline_state: None,
            table_replication_states: HashMap::new(),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }
}

impl Default for MemoryPipelineStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineStateStore for MemoryPipelineStateStore {
    async fn load_pipeline_state<I>(
        &self,
        pipeline_id: &I,
    ) -> Result<PipelineState, PipelineStateStoreError>
    where
        I: PartialEq + Send + Sync + 'static,
        PipelineState: Borrow<I>,
    {
        let inner = self.inner.read().await;
        match &inner.pipeline_state {
            Some(state) if state.borrow() == pipeline_id => Ok(state.clone()),
            _ => Err(PipelineStateStoreError::PipelineStateNotFound),
        }
    }

    async fn store_pipeline_state(
        &self,
        state: PipelineState,
        overwrite: bool,
    ) -> Result<bool, PipelineStateStoreError> {
        let mut inner = self.inner.write().await;

        if !overwrite && inner.pipeline_state.is_some() {
            return Ok(false);
        }

        inner.pipeline_state = Some(state);
        Ok(true)
    }

    async fn load_table_replication_state<I>(
        &self,
        table_id: &I,
    ) -> Result<Option<TableReplicationState>, PipelineStateStoreError>
    where
        I: PartialEq + Send + Sync + 'static,
        TableReplicationState: Borrow<I>,
    {
        let inner = self.inner.read().await;
        Ok(inner
            .table_replication_states
            .values()
            .find(|state| state.borrow() == table_id)
            .cloned())
    }

    async fn load_table_replication_states(
        &self,
    ) -> Result<Vec<TableReplicationState>, PipelineStateStoreError> {
        let inner = self.inner.read().await;
        Ok(inner.table_replication_states.values().cloned().collect())
    }

    async fn store_table_replication_state(
        &self,
        state: TableReplicationState,
        overwrite: bool,
    ) -> Result<bool, PipelineStateStoreError> {
        let mut inner = self.inner.write().await;

        if !overwrite && inner.table_replication_states.contains_key(&state.id) {
            return Ok(false);
        }

        inner.table_replication_states.insert(state.id, state);
        Ok(true)
    }
}
