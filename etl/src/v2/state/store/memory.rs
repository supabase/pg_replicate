use crate::v2::state::pipeline::PipelineState;
use crate::v2::state::relation_subscription::RelationSubscriptionState;

#[derive(Debug)]
pub struct MemoryPipelineStateStore {
    pipeline: Option<PipelineState>,
    relation_subscriptions: Vec<RelationSubscriptionState>,
}

impl MemoryPipelineStateStore {
    pub fn new() -> Self {
        todo!()
    }
}
