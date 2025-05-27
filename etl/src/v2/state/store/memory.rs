use crate::v2::state::pipeline::PipelineState;
use crate::v2::state::relation_subscription::TableReplicationState;

#[derive(Debug)]
pub struct MemoryPipelineStateStore {
    pipeline: Option<PipelineState>,
    relation_subscriptions: Vec<TableReplicationState>,
}

impl MemoryPipelineStateStore {
    pub fn new() -> Self {
        todo!()
    }
}
