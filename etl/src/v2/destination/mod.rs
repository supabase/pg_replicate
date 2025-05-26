use crate::conversions::cdc_event::CdcEvent;
use async_trait::async_trait;

#[async_trait]
pub trait Destination {
    async fn apply_events(events: Vec<CdcEvent>);
}
