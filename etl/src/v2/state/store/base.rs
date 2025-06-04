use postgres::schema::{Oid, TableSchema};
use std::future::Future;
use thiserror::Error;

use crate::v2::pipeline::PipelineId;
use crate::v2::state::origin::ReplicationOriginState;
use crate::v2::state::table::TableReplicationState;

#[derive(Debug, Error)]
pub enum StateStoreError {
    #[error("Pipeline state not found in store: state does not exist")]
    PipelineStateNotFound,

    #[error("Table replication state not found in store: state does not exist")]
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

    fn load_table_schemas(
        &self,
        pipeline_id: PipelineId,
    ) -> impl Future<Output = Result<Vec<TableSchema>, StateStoreError>> + Send;

    fn load_table_schema(
        &self,
        pipeline_id: PipelineId,
        table_id: Oid,
    ) -> impl Future<Output = Result<Option<TableSchema>, StateStoreError>> + Send;

    fn store_table_schema(
        &self,
        pipeline_id: PipelineId,
        table_schema: TableSchema,
        overwrite: bool,
    ) -> impl Future<Output = Result<bool, StateStoreError>> + Send;
}
