use etl::conversions::cdc_event::CdcEvent;
use etl::conversions::table_row::TableRow;
use etl::v2::destination::base::{Destination, DestinationError};
use postgres::schema::{Oid, TableName, TableSchema};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{Notify, RwLock};

type EventCondition = Box<dyn Fn(&[Arc<CdcEvent>]) -> bool + Send + Sync>;
type SchemaCondition = Box<dyn Fn(&[TableSchema]) -> bool + Send + Sync>;
type TableRowCondition = Box<dyn Fn(&HashMap<Oid, Vec<TableRow>>) -> bool + Send + Sync>;

struct Inner {
    events: Vec<Arc<CdcEvent>>,
    table_schemas: Vec<TableSchema>,
    table_rows: HashMap<Oid, Vec<TableRow>>,
    event_conditions: Vec<(EventCondition, Arc<Notify>)>,
    table_schema_conditions: Vec<(SchemaCondition, Arc<Notify>)>,
    table_row_conditions: Vec<(TableRowCondition, Arc<Notify>)>,
}

#[derive(Clone)]
pub struct TestDestination {
    inner: Arc<RwLock<Inner>>,
}

impl fmt::Debug for TestDestination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = tokio::task::block_in_place(move || {
            Handle::current().block_on(async move { self.inner.read().await })
        });
        f.debug_struct("TestDestination")
            .field("events", &inner.events)
            .field("schemas", &inner.table_schemas)
            .field("table_rows", &inner.table_rows)
            .finish()
    }
}

impl TestDestination {
    pub fn new() -> Self {
        let inner = Inner {
            events: Vec::new(),
            table_schemas: Vec::new(),
            table_rows: HashMap::new(),
            event_conditions: Vec::new(),
            table_schema_conditions: Vec::new(),
            table_row_conditions: Vec::new(),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub async fn get_events(&self) -> Vec<Arc<CdcEvent>> {
        self.inner.read().await.events.clone()
    }

    pub async fn get_table_schemas(&self) -> Vec<TableSchema> {
        self.inner.read().await.table_schemas.clone()
    }

    pub async fn get_table_rows(&self) -> HashMap<Oid, Vec<TableRow>> {
        self.inner.read().await.table_rows.clone()
    }

    pub async fn clear(&self) {
        let mut inner = self.inner.write().await;
        inner.events.clear();
        inner.table_schemas.clear();
        inner.table_rows.clear();
    }

    pub async fn notify_on_events<F>(&self, condition: F) -> Arc<Notify>
    where
        F: Fn(&[Arc<CdcEvent>]) -> bool + Send + Sync + 'static,
    {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner
            .event_conditions
            .push((Box::new(condition), notify.clone()));

        notify
    }

    pub async fn notify_on_schemas<F>(&self, condition: F) -> Arc<Notify>
    where
        F: Fn(&[TableSchema]) -> bool + Send + Sync + 'static,
    {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner
            .table_schema_conditions
            .push((Box::new(condition), notify.clone()));

        notify
    }

    pub async fn notify_on_table_rows<F>(&self, condition: F) -> Arc<Notify>
    where
        F: Fn(&HashMap<Oid, Vec<TableRow>>) -> bool + Send + Sync + 'static,
    {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner
            .table_row_conditions
            .push((Box::new(condition), notify.clone()));

        notify
    }

    pub async fn wait_for_schemas(&self, table_names: Vec<TableName>) -> Arc<Notify> {
        self.notify_on_schemas(move |schemas| {
            table_names
                .iter()
                .all(|required_name| schemas.iter().any(|schema| schema.name == *required_name))
        })
        .await
    }

    pub async fn wait_for_n_schemas(&self, n: usize) -> Arc<Notify> {
        self.notify_on_schemas(move |schemas| schemas.len() == n)
            .await
    }

    pub async fn refresh(&self) {
        self.check_conditions().await;
    }

    async fn check_conditions(&self) {
        let mut inner = self.inner.write().await;

        // Check event conditions
        let events = inner.events.clone();
        inner.event_conditions.retain(|(condition, notify)| {
            let should_retain = !condition(&events);
            if !should_retain {
                notify.notify_one();
            }
            should_retain
        });

        // Check schema conditions
        let schemas = inner.table_schemas.clone();
        inner.table_schema_conditions.retain(|(condition, notify)| {
            let should_retain = !condition(&schemas);
            if !should_retain {
                notify.notify_one();
            }
            should_retain
        });

        // Check table row conditions
        let table_rows = inner.table_rows.clone();
        inner.table_row_conditions.retain(|(condition, notify)| {
            let should_retain = !condition(&table_rows);
            if !should_retain {
                notify.notify_one();
            }
            should_retain
        });
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
        inner.table_schemas.push(schema);
        drop(inner); // Release the write lock before checking conditions

        self.check_conditions().await;

        Ok(())
    }

    async fn copy_table_rows(&self, id: Oid, rows: Vec<TableRow>) -> Result<(), DestinationError> {
        let mut inner = self.inner.write().await;
        inner.table_rows.insert(id, rows);
        drop(inner); // Release the write lock before checking conditions

        self.check_conditions().await;

        Ok(())
    }

    async fn apply_events(&self, events: Vec<CdcEvent>) -> Result<(), DestinationError> {
        let mut inner = self.inner.write().await;
        let arc_events = events.into_iter().map(Arc::new).collect::<Vec<_>>();
        inner.events.extend(arc_events);
        drop(inner); // Release the write lock before checking conditions

        self.check_conditions().await;

        Ok(())
    }
}
