use crate::v2::state::pipeline::PipelineState;
use crate::v2::state::relation_subscription::RelationSubscriptionState;
use postgres::schema::Oid;
use std::future::Future;

pub trait PipelineStateStore {
    fn load_pipeline_state(&self) -> impl Future<Output = PipelineState> + Send;

    fn store_pipeline_state(&self, state: PipelineState) -> impl Future<Output = ()> + Send;

    fn load_relation_subscription_states(
        &self,
    ) -> impl Future<Output = Vec<RelationSubscriptionState>> + Send;

    fn load_relation_subscription_state(
        &self,
        rel_id: &Oid,
    ) -> impl Future<Output = RelationSubscriptionState> + Send;

    fn store_relation_subscription_state(
        &self,
        state: RelationSubscriptionState,
    ) -> impl Future<Output = ()> + Send;
}
