use postgres::schema::Oid;
use std::future::Future;
use thiserror::Error;

use crate::v2::pipeline::PipelineId;
use crate::v2::state::origin::ReplicationOriginState;
use crate::v2::state::table::TableReplicationState;

#[derive(Debug, Error)]
pub enum StateStoreError {
    #[error("Replication origin state not found in store")]
    ReplicationOriginStateNotFound,

    #[error("Table replication state not found in store")]
    TableReplicationStateNotFound,
}

pub trait StateStore {
    fn load_replication_origin_state(
        &self,
        pipeline_id: PipelineId,
        table_id: Option<Oid>,
    ) -> impl Future<Output = Result<Option<ReplicationOriginState>, StateStoreError>> + Send;

    fn store_replication_origin_state(
        &self,
        state: ReplicationOriginState,
        overwrite: bool,
    ) -> impl Future<Output = Result<bool, StateStoreError>> + Send;

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
        overwrite: bool,
    ) -> impl Future<Output = Result<bool, StateStoreError>> + Send;
}
