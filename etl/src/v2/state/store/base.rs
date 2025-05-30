use crate::v2::state::pipeline::PipelineState;
use crate::v2::state::table::TableReplicationState;
use postgres::schema::Oid;
use std::future::Future;

pub trait PipelineStateStore {
    fn load_pipeline_state(&self) -> impl Future<Output = PipelineState> + Send;

    fn store_pipeline_state(&self, state: PipelineState) -> impl Future<Output = ()> + Send;

    fn load_table_replication_states(
        &self,
    ) -> impl Future<Output = Vec<TableReplicationState>> + Send;

    fn load_table_replication_state(
        &self,
        table_id: &Oid,
    ) -> impl Future<Output = Option<TableReplicationState>> + Send;

    fn store_table_replication_state(
        &self,
        state: TableReplicationState,
    ) -> impl Future<Output = ()> + Send;
}
