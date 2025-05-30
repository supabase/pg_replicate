use postgres::schema::Oid;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::v2::state::pipeline::PipelineState;
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::state::table::TableReplicationState;

#[derive(Debug)]
struct Inner {
    pipeline_state: PipelineState,
    table_replication_states: HashMap<Oid, TableReplicationState>,
}

#[derive(Debug, Clone)]
pub struct MemoryPipelineStateStore {
    inner: Arc<RwLock<Inner>>,
}

impl MemoryPipelineStateStore {
    pub fn new() -> Self {
        let inner = Inner {
            pipeline_state: PipelineState::default(),
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
    async fn load_pipeline_state(&self) -> PipelineState {
        self.inner.read().await.pipeline_state.clone()
    }

    async fn store_pipeline_state(&self, state: PipelineState) {
        self.inner.write().await.pipeline_state = state;
    }

    async fn load_table_replication_states(&self) -> Vec<TableReplicationState> {
        self.inner
            .read()
            .await
            .table_replication_states
            .values()
            .cloned()
            .collect()
    }

    async fn load_table_replication_state(&self, table_id: &Oid) -> Option<TableReplicationState> {
        self.inner
            .read()
            .await
            .table_replication_states
            .get(table_id)
            .cloned()
    }

    async fn store_table_replication_state(&self, state: TableReplicationState) {
        self.inner
            .write()
            .await
            .table_replication_states
            .insert(state.id, state);
    }
}
