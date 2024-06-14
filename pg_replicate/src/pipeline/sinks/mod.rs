use std::collections::HashMap;

use async_trait::async_trait;
use gcp_bigquery_client::error::BQError;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio_postgres::types::PgLsn;

use crate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    table::{TableId, TableSchema},
};

use self::duckdb::{DuckDbExecutorError, DuckDbRequest};

use super::PipelineResumptionState;

pub mod bigquery;
pub mod duckdb;
pub mod stdout;

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("failed to send duckdb request")]
    SendError(#[from] SendError<DuckDbRequest>),

    #[error("duckdb executor error: {0}")]
    DuckDbExecutor(#[from] DuckDbExecutorError),

    #[error("no response received")]
    NoResponseReceived,

    #[error("bigquery error: {0}")]
    BigQuery(#[from] BQError)
}

#[async_trait]
pub trait Sink {
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, SinkError>;
    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), SinkError>;
    async fn write_table_row(&mut self, row: TableRow, table_id: TableId) -> Result<(), SinkError>;
    async fn write_cdc_event(&mut self, event: CdcEvent) -> Result<PgLsn, SinkError>;
    async fn table_copied(&mut self, table_id: TableId) -> Result<(), SinkError>;
    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), SinkError>;
}
