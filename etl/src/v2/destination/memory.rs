use crate::conversions::cdc_event::CdcEvent;
use crate::v2::destination::base::Destination;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
struct Inner {
    events: Vec<CdcEvent>,
}

#[derive(Debug, Clone)]
pub struct MemoryDestination {
    inner: Arc<RwLock<Inner>>,
}

impl MemoryDestination {
    pub fn new() -> Self {
        let inner = Inner { events: Vec::new() };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }
}

impl Destination for MemoryDestination {
    async fn apply_events(&self, events: Vec<CdcEvent>) {}
}
