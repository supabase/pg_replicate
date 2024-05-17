use std::collections::HashMap;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

use crate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    table::{TableId, TableSchema},
};

use self::duckdb::DuckDbRequest;

pub mod duckdb;
pub mod stdout;

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("failed to send duckdb request")]
    SendError(#[from] SendError<DuckDbRequest>),
}

#[async_trait]
pub trait Sink {
    async fn write_table_schemas(
        &self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), SinkError>;
    async fn write_table_row(&self, row: TableRow, table_id: TableId) -> Result<(), SinkError>;
    async fn write_cdc_event(&self, event: CdcEvent) -> Result<(), SinkError>;
}
