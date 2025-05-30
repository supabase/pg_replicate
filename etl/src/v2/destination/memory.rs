use crate::conversions::cdc_event::CdcEvent;
use crate::v2::destination::base::Destination;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
struct Inner {
    _events: Vec<CdcEvent>,
}

#[derive(Debug, Clone)]
pub struct MemoryDestination {
    _inner: Arc<RwLock<Inner>>,
}

impl MemoryDestination {
    pub fn new() -> Self {
        let inner = Inner {
            _events: Vec::new(),
        };

        Self {
            _inner: Arc::new(RwLock::new(inner)),
        }
    }
}

impl Default for MemoryDestination {
    fn default() -> Self {
        Self::new()
    }
}

impl Destination for MemoryDestination {
    async fn apply_events(&self, _events: Vec<CdcEvent>) {}
}
