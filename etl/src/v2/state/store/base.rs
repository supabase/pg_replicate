use postgres::schema::TableId;
use std::{collections::HashMap, future::Future};
use thiserror::Error;

use crate::v2::state::table::TableReplicationState;

#[derive(Debug, Error)]
pub enum StateStoreError {
    #[error("Sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
}

pub trait StateStore {
    fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = Result<Option<TableReplicationState>, StateStoreError>> + Send;

    fn load_table_replication_states(
        &self,
    ) -> impl Future<Output = Result<HashMap<TableId, TableReplicationState>, StateStoreError>> + Send;

    fn store_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationState,
    ) -> impl Future<Output = Result<(), StateStoreError>> + Send;
}
