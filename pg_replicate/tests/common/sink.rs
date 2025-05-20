use async_trait::async_trait;
use pg_replicate::conversions::cdc_event::CdcEvent;
use pg_replicate::conversions::table_row::TableRow;
use pg_replicate::pipeline::sinks::{BatchSink, InfallibleSinkError};
use pg_replicate::pipeline::PipelineResumptionState;
use postgres::schema::{TableId, TableSchema};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio_postgres::types::PgLsn;

#[derive(Debug, Clone)]
pub struct TestSink {
    // We use Arc<Mutex> to allow the sink to be shared by multiple pipelines, effectively
    // simulating recreating pipelines with a sink that "persists" data.
    inner: Arc<Mutex<TestSinkInner>>,
}

#[derive(Debug)]
struct TestSinkInner {
    // We have a Vec to store all the changes of the schema that we receive over time.
    tables_schemas: Vec<HashMap<TableId, TableSchema>>,
    tables_rows: HashMap<TableId, Vec<TableRow>>,
    events: Vec<Arc<CdcEvent>>,
    tables_copied: u8,
    tables_truncated: u8,
}

impl TestSink {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(TestSinkInner {
                tables_schemas: Vec::new(),
                tables_rows: HashMap::new(),
                events: Vec::new(),
                tables_copied: 0,
                tables_truncated: 0,
            })),
        }
    }

    pub fn get_tables_schemas(&self) -> Vec<HashMap<TableId, TableSchema>> {
        self.inner.lock().unwrap().tables_schemas.clone()
    }

    pub fn get_tables_rows(&self) -> HashMap<TableId, Vec<TableRow>> {
        self.inner.lock().unwrap().tables_rows.clone()
    }

    pub fn get_events(&self) -> Vec<Arc<CdcEvent>> {
        self.inner.lock().unwrap().events.clone()
    }

    pub fn get_tables_copied(&self) -> u8 {
        self.inner.lock().unwrap().tables_copied
    }

    pub fn get_tables_truncated(&self) -> u8 {
        self.inner.lock().unwrap().tables_truncated
    }
}

#[async_trait]
impl BatchSink for TestSink {
    type Error = InfallibleSinkError;

    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        Ok(PipelineResumptionState {
            copied_tables: HashSet::new(),
            last_lsn: PgLsn::from(0),
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        self.inner
            .lock()
            .unwrap()
            .tables_schemas
            .push(table_schemas);
        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        self.inner
            .lock()
            .unwrap()
            .tables_rows
            .entry(table_id)
            .or_default()
            .extend(rows);
        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        // Since CdcEvent is not Clone, we have to wrap it in an Arc, and we are fine with this
        // since it's not mutable, so we don't even have to use mutexes.
        let arc_events = events.into_iter().map(Arc::new).collect::<Vec<_>>();
        self.inner.lock().unwrap().events.extend(arc_events);
        Ok(PgLsn::from(0))
    }

    async fn table_copied(&mut self, _table_id: TableId) -> Result<(), Self::Error> {
        self.inner.lock().unwrap().tables_copied += 1;
        Ok(())
    }

    async fn truncate_table(&mut self, _table_id: TableId) -> Result<(), Self::Error> {
        self.inner.lock().unwrap().tables_truncated += 1;
        Ok(())
    }
}
