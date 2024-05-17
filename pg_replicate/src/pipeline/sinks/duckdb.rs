use std::{collections::HashMap, path::Path};

use tokio::sync::mpsc::{channel, Receiver, Sender};

use async_trait::async_trait;
use tracing::{error, info};

use crate::{
    clients::duckdb::DuckDbClient,
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    table::{TableId, TableSchema},
};

use super::{Sink, SinkError};

pub enum DuckDbRequest {
    CreateTables(HashMap<TableId, TableSchema>),
    InsertRow(TableRow, TableId),
}

struct DuckDbExecutor {
    client: DuckDbClient,
    receiver: Receiver<DuckDbRequest>,
    table_schemas: Option<HashMap<TableId, TableSchema>>,
}

impl DuckDbExecutor {
    pub fn start(mut self) {
        tokio::spawn(async move {
            while let Some(req) = self.receiver.recv().await {
                match req {
                    DuckDbRequest::CreateTables(table_schemas) => {
                        self.create_tables(&table_schemas);
                        self.table_schemas = Some(table_schemas);
                    }
                    DuckDbRequest::InsertRow(row, table_id) => {
                        self.insert_row(row, table_id);
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

    fn insert_row(&self, table_row: TableRow, table_id: TableId) {
        let table_schema = self
            .table_schemas
            .as_ref()
            .expect("missing table schemas while inserting a row")
            .get(&table_id)
            .expect("missing table id while inserting a row");

        match self.client.insert_row(&table_schema.table_name, &table_row) {
            Ok(_) => {
                info!("inserted row");
            }
            Err(e) => {
                error!("DuckDb error: {e}");
            }
        }
    }
}

pub struct DuckDbSink {
    sender: Sender<DuckDbRequest>,
}

impl DuckDbSink {
    pub async fn file<P: AsRef<Path>>(file_name: P) -> Result<DuckDbSink, duckdb::Error> {
        let (sender, receiver) = channel(32);
        let client = DuckDbClient::open_file(file_name)?;
        let executor = DuckDbExecutor {
            client,
            receiver,
            table_schemas: None,
        };
        executor.start();
        Ok(DuckDbSink { sender })
    }

    pub async fn in_memory() -> Result<DuckDbSink, duckdb::Error> {
        let (sender, receiver) = channel(32);
        let client = DuckDbClient::open_in_memory()?;
        let executor = DuckDbExecutor {
            client,
            receiver,
            table_schemas: None,
        };
        executor.start();
        Ok(DuckDbSink { sender })
    }
}

#[async_trait]
impl Sink for DuckDbSink {
    async fn write_table_schemas(
        &self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), SinkError> {
        let req = DuckDbRequest::CreateTables(table_schemas);
        self.sender.send(req).await?;
        Ok(())
    }

    async fn write_table_row(&self, row: TableRow, table_id: TableId) -> Result<(), SinkError> {
        let req = DuckDbRequest::InsertRow(row, table_id);
        self.sender.send(req).await?;
        Ok(())
    }

    async fn write_cdc_event(&self, _event: CdcEvent) -> Result<(), SinkError> {
        // self.tx.send(43).await.expect("failed to send number");
        Ok(())
    }
}
