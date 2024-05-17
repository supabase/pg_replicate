use std::collections::HashMap;

use tokio::sync::mpsc::{channel, Receiver, Sender};

use async_trait::async_trait;
use tracing::{error, info};

use crate::{
    clients::duckdb::DuckDbClient,
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    table::TableSchema,
};

use super::{Sink, SinkError};

enum DuckDbRequest {
    CreateTables(HashMap<u32, TableSchema>),
}

struct DuckDbExecutor {
    pub client: DuckDbClient,
    pub receiver: Receiver<DuckDbRequest>,
}

impl DuckDbExecutor {
    pub fn start(mut self) {
        tokio::spawn(async move {
            while let Some(req) = self.receiver.recv().await {
                match req {
                    DuckDbRequest::CreateTables(table_schemas) => {
                        self.create_tables(&table_schemas);
                    }
                }
            }
        });
    }

    fn create_tables(&self, table_schemas: &HashMap<u32, TableSchema>) {
        for table_schema in table_schemas.values() {
            let schema = &table_schema.table_name.schema;

            match self.client.create_schema_if_missing(schema) {
                Ok(_) => {
                    info!("created schema '{schema}'");
                }
                Err(e) => {
                    error!("DuckDb error: {e}");
                    return;
                }
            }

            match self.client.create_table_if_missing(table_schema) {
                Ok(_) => {
                    info!(
                        "created table '{}.{}'",
                        table_schema.table_name.schema, table_schema.table_name.name
                    );
                }
                Err(e) => {
                    error!("DuckDb error: {e}");
                }
            }
        }
    }
}

pub struct DuckDbSink {
    tx: Sender<DuckDbRequest>,
}

impl DuckDbSink {
    pub async fn new() -> Result<DuckDbSink, duckdb::Error> {
        let (sender, receiver) = channel(32);
        let client = DuckDbClient::open_in_memory()?;
        let executor = DuckDbExecutor { client, receiver };
        executor.start();
        Ok(DuckDbSink { tx: sender })
    }
}

#[async_trait]
impl Sink for DuckDbSink {
    async fn write_table_schemas(
        &self,
        table_schemas: HashMap<u32, TableSchema>,
    ) -> Result<(), SinkError> {
        let req = DuckDbRequest::CreateTables(table_schemas);
        self.tx.send(req).await.expect("failed to send number");
        Ok(())
    }

    async fn write_table_row(&self, _row: TableRow) -> Result<(), SinkError> {
        // self.tx.send(42).await.expect("failed to send number");
        Ok(())
    }

    async fn write_cdc_event(&self, _event: CdcEvent) -> Result<(), SinkError> {
        // self.tx.send(43).await.expect("failed to send number");
        Ok(())
    }
}
