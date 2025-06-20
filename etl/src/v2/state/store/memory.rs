use postgres::schema::{Oid, TableId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::v2::state::store::base::{StateStore, StateStoreError};
use crate::v2::state::table::TableReplicationState;

#[derive(Debug)]
struct Inner {
    table_replication_states: HashMap<Oid, TableReplicationState>,
}

#[derive(Debug, Clone)]
pub struct MemoryStateStore {
    inner: Arc<RwLock<Inner>>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        let inner = Inner {
            table_replication_states: HashMap::new(),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }
}

impl Default for MemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for MemoryStateStore {
    async fn get_table_replication_state(
        &self,
        table_id: Oid,
    ) -> Result<Option<TableReplicationState>, StateStoreError> {
        let inner = self.inner.read().await;

        Ok(inner.table_replication_states.get(&table_id).cloned())
    }

    async fn load_table_replication_states(
        &self,
    ) -> Result<Vec<TableReplicationState>, StateStoreError> {
        let inner = self.inner.read().await;

        Ok(inner.table_replication_states.values().cloned().collect())
    }

    async fn store_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationState,
    ) -> Result<(), StateStoreError> {
        let mut inner = self.inner.write().await;
        inner.table_replication_states.insert(table_id, state);
        Ok(())
    }
}
