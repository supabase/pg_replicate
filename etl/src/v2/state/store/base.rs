use crate::v2::state::pipeline::PipelineState;
use crate::v2::state::table::TableReplicationState;
use std::borrow::Borrow;
use std::future::Future;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PipelineStateStoreError {
    #[error("The pipeline state was not found")]
    PipelineStateNotFound,

    #[error("The table replication state was not found")]
    TableReplicationStateNotFound,
}

pub trait PipelineStateStore {
    fn load_pipeline_state<I>(
        &self,
        pipeline_id: &I,
    ) -> impl Future<Output = Result<PipelineState, PipelineStateStoreError>> + Send
    where
        I: PartialEq + Send + Sync + 'static,
        PipelineState: Borrow<I>;

    fn store_pipeline_state(
        &self,
        state: PipelineState,
        overwrite: bool,
    ) -> impl Future<Output = Result<bool, PipelineStateStoreError>> + Send;

    fn load_table_replication_state<I>(
        &self,
        table_id: &I,
    ) -> impl Future<Output = Result<Option<TableReplicationState>, PipelineStateStoreError>> + Send
    where
        I: PartialEq + Send + Sync + 'static,
        TableReplicationState: Borrow<I>;

    fn load_table_replication_states(
        &self,
    ) -> impl Future<Output = Result<Vec<TableReplicationState>, PipelineStateStoreError>> + Send;

    fn store_table_replication_state(
        &self,
        state: TableReplicationState,
        overwrite: bool,
    ) -> impl Future<Output = Result<bool, PipelineStateStoreError>> + Send;
}
