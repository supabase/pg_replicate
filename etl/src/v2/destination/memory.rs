use crate::conversions::cdc_event::CdcEvent;
use crate::conversions::table_row::TableRow;
use crate::v2::destination::base::{Destination, DestinationError};
use postgres::schema::{Oid, TableSchema};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
struct Inner {
    events: Vec<CdcEvent>,
    schemas: Vec<TableSchema>,
    table_rows: Vec<(Oid, Vec<TableRow>)>,
}

#[derive(Debug, Clone)]
pub struct MemoryDestination {
    inner: Arc<RwLock<Inner>>,
}

impl MemoryDestination {
    pub fn new() -> Self {
        let inner = Inner {
            events: Vec::new(),
            schemas: Vec::new(),
            table_rows: Vec::new(),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }
}

impl Default for MemoryDestination {
    fn default() -> Self {
        Self::new()
    }
}

impl Destination for MemoryDestination {
    async fn write_table_schema(&self, schema: TableSchema) -> Result<(), DestinationError> {
        let mut inner = self.inner.write().await;
        inner.schemas.push(schema);
        Ok(())
    }

    async fn copy_table_rows(&self, id: Oid, rows: Vec<TableRow>) -> Result<(), DestinationError> {
        let mut inner = self.inner.write().await;
        inner.table_rows.push((id, rows));
        Ok(())
    }

    async fn apply_events(&self, events: Vec<CdcEvent>) -> Result<(), DestinationError> {
        let mut inner = self.inner.write().await;
        inner.events.extend(events);
        Ok(())
    }
}
