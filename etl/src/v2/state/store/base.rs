use postgres::schema::Oid;
use std::future::Future;
use thiserror::Error;

use crate::v2::pipeline::PipelineId;
use crate::v2::state::table::TableReplicationState;

#[derive(Debug, Error)]
pub enum StateStoreError {
    #[error("Sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
}

pub trait StateStore {
    fn load_table_replication_state(
        &self,
        pipeline_id: PipelineId,
        table_id: Oid,
    ) -> impl Future<Output = Result<Option<TableReplicationState>, StateStoreError>> + Send;

    fn load_table_replication_states(
        &self,
    ) -> impl Future<Output = Result<Vec<TableReplicationState>, StateStoreError>> + Send;

    fn store_table_replication_state(
        &self,
        state: TableReplicationState,
    ) -> impl Future<Output = Result<(), StateStoreError>> + Send;
}
