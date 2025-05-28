use crate::conversions::cdc_event::CdcEvent;
use std::future::Future;

pub trait Destination {
    fn apply_events(&self, events: Vec<CdcEvent>) -> impl Future<Output = ()> + Send;
}
