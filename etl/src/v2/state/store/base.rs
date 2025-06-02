use postgres::schema::{Oid, TableSchema};
use std::future::Future;
use thiserror::Error;

use crate::v2::pipeline::PipelineId;
use crate::v2::state::pipeline::PipelineState;
use crate::v2::state::table::TableReplicationState;

#[derive(Debug, Error)]
pub enum PipelineStateStoreError {
    #[error("Pipeline state not found in store: state does not exist")]
    PipelineStateNotFound,

    #[error("Table replication state not found in store: state does not exist")]
    TableReplicationStateNotFound,
}

pub trait PipelineStateStore {
    fn load_pipeline_state(
        &self,
        pipeline_id: PipelineId,
    ) -> impl Future<Output = Result<PipelineState, PipelineStateStoreError>> + Send;

    fn store_pipeline_state(
        &self,
        state: PipelineState,
        overwrite: bool,
    ) -> impl Future<Output = Result<bool, PipelineStateStoreError>> + Send;

    fn load_table_replication_state(
        &self,
        pipeline_id: PipelineId,
        table_id: Oid,
    ) -> impl Future<Output = Result<Option<TableReplicationState>, PipelineStateStoreError>> + Send;

    fn load_table_replication_states(
        &self,
    ) -> impl Future<Output = Result<Vec<TableReplicationState>, PipelineStateStoreError>> + Send;

    fn store_table_replication_state(
        &self,
        pipeline_id: PipelineId,
        state: TableReplicationState,
        overwrite: bool,
    ) -> impl Future<Output = Result<bool, PipelineStateStoreError>> + Send;

    fn load_table_schemas(
        &self,
        pipeline_id: PipelineId,
    ) -> impl Future<Output = Result<Vec<TableSchema>, PipelineStateStoreError>> + Send;

    fn load_table_schema(
        &self,
        pipeline_id: PipelineId,
        table_id: Oid,
    ) -> impl Future<Output = Result<Option<TableSchema>, PipelineStateStoreError>> + Send;

    fn store_table_schema(
        &self,
        pipeline_id: PipelineId,
        table_schema: TableSchema,
        overwrite: bool,
    ) -> impl Future<Output = Result<bool, PipelineStateStoreError>> + Send;
}
