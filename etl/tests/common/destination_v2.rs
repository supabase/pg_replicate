use etl::conversions::cdc_event::CdcEvent;
use etl::conversions::table_row::TableRow;
use etl::v2::destination::base::{Destination, DestinationError};
use postgres::schema::{Oid, TableSchema};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
struct Inner {
    events: Vec<Arc<CdcEvent>>,
    schemas: Vec<TableSchema>,
    table_rows: Vec<(Oid, Vec<TableRow>)>,
}

#[derive(Debug, Clone)]
pub struct TestDestination {
    inner: Arc<RwLock<Inner>>,
}

impl TestDestination {
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

    pub async fn get_events(&self) -> Vec<Arc<CdcEvent>> {
        self.inner.read().await.events.clone()
    }

    pub async fn get_schemas(&self) -> Vec<TableSchema> {
        self.inner.read().await.schemas.clone()
    }

    pub async fn get_table_rows(&self) -> Vec<(Oid, Vec<TableRow>)> {
        self.inner.read().await.table_rows.clone()
    }

    pub async fn clear(&self) {
        let mut inner = self.inner.write().await;
        inner.events.clear();
        inner.schemas.clear();
        inner.table_rows.clear();
    }
}

impl Default for TestDestination {
    fn default() -> Self {
        Self::new()
    }
}

impl Destination for TestDestination {
    async fn write_table_schema(&self, schema: TableSchema) -> Result<(), DestinationError> {
        let mut inner = self.inner.write().await;
        inner.schemas.push(schema);

        Ok(())
    }

    async fn copy_table(&self, id: Oid, rows: Vec<TableRow>) -> Result<(), DestinationError> {
        let mut inner = self.inner.write().await;
        inner.table_rows.push((id, rows));

        Ok(())
    }

    async fn apply_events(&self, events: Vec<CdcEvent>) -> Result<(), DestinationError> {
        let mut inner = self.inner.write().await;
        let arc_events = events.into_iter().map(Arc::new).collect::<Vec<_>>();
        inner.events.extend(arc_events);

        Ok(())
    }
}
