use crate::v2::state::pipeline::PipelineState;
use crate::v2::state::relation_subscription::RelationSubscriptionState;

pub trait PipelineStateStore {
    async fn load_pipeline_state(&self) -> PipelineState;

    async fn store_pipeline_state(&self, state: PipelineState);

    async fn load_relation_subscription_states(&self) -> Vec<RelationSubscriptionState>;

    async fn store_relation_subscription_state(&self, state: RelationSubscriptionState);
}
