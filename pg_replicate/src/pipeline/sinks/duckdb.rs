use std::collections::HashMap;

use tokio::sync::mpsc::{channel, Receiver, Sender};

use async_trait::async_trait;
use duckdb::Connection;
use tracing::info;

use crate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    table::TableSchema,
};

use super::{Sink, SinkError};

pub struct DuckDbExecutor {
    pub conn: Connection,
    pub receiver: Receiver<i32>,
}

impl DuckDbExecutor {
    pub fn start(mut self) {
        tokio::spawn(async move {
            while let Some(n) = self.receiver.recv().await {
                let x = self
                    .conn
                    .execute("select 1", [])
                    .expect("failed to execute select query");
                info!("Got number: {n}, {x}");
            }
        });
    }
}

pub struct DuckDbSink {
    pub tx: Sender<i32>,
}

impl DuckDbSink {
    pub async fn new() -> Result<DuckDbSink, duckdb::Error> {
        let (sender, receiver) = channel(32);
        let conn = Connection::open_in_memory()?;
        let executor = DuckDbExecutor { conn, receiver };
        executor.start();
        Ok(DuckDbSink { tx: sender })
    }
}

#[async_trait]
impl Sink for DuckDbSink {
    async fn write_table_schemas(
        &self,
        _table_schemas: &HashMap<u32, TableSchema>,
    ) -> Result<(), SinkError> {
        self.tx.send(41).await.expect("failed to send number");
        Ok(())
    }

    async fn write_table_row(&self, _row: TableRow) -> Result<(), SinkError> {
        self.tx.send(42).await.expect("failed to send number");
        Ok(())
    }

    async fn write_cdc_event(&self, _event: CdcEvent) -> Result<(), SinkError> {
        self.tx.send(43).await.expect("failed to send number");
        Ok(())
    }
}
