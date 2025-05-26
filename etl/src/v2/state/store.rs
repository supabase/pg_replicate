use crate::v2::state::pipeline::PipelineState;
use crate::v2::state::relation_subscription::RelationSubscriptionState;
use async_trait::async_trait;

// The main state that we have comprises:
// - Pipeline state
//  - stores all the required information for properly starting up a pipeline
// - Relation subscription
//  - stores all the state which is used by the apply work to spawn table sync workers that copy
//      table data

#[async_trait]
pub trait PipelineStateStore {
    async fn load_pipeline() -> PipelineState;

    async fn update_pipeline(state: PipelineState);

    async fn load_relation_subscriptions() -> Vec<RelationSubscriptionState>;

    async fn update_relation_subscription(state: RelationSubscriptionState);
}
