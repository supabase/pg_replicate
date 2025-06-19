use postgres::schema::Oid;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::v2::pipeline::PipelineId;
use crate::v2::state::store::base::{StateStore, StateStoreError};
use crate::v2::state::table::TableReplicationState;

#[derive(Debug)]
struct Inner {
    table_replication_states: HashMap<(PipelineId, Oid), TableReplicationState>,
}

#[derive(Debug, Clone)]
pub struct MemoryStateStore {
    inner: Arc<RwLock<Inner>>,
    pipeline_id: PipelineId,
}

impl MemoryStateStore {
    pub fn new(pipeline_id: PipelineId) -> Self {
        let inner = Inner {
            table_replication_states: HashMap::new(),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
            pipeline_id,
        }
    }
}

impl StateStore for MemoryStateStore {
    async fn load_table_replication_state(
        &self,
        table_id: Oid,
    ) -> Result<Option<TableReplicationState>, StateStoreError> {
        let inner = self.inner.read().await;

        Ok(inner
            .table_replication_states
            .get(&(self.pipeline_id, table_id))
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
        state: TableReplicationState,
    ) -> Result<(), StateStoreError> {
        let mut inner = self.inner.write().await;
        let key = (state.pipeline_id, state.table_id);
        inner.table_replication_states.insert(key, state);
        Ok(())
    }
}
