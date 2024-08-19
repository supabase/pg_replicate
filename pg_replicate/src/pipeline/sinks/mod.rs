use std::collections::HashMap;

use async_trait::async_trait;
#[cfg(feature = "bigquery")]
use gcp_bigquery_client::error::BQError;
use thiserror::Error;
#[cfg(feature = "duckdb")]
use tokio::sync::mpsc::error::SendError;
use tokio_postgres::types::PgLsn;

#[cfg(feature = "bigquery")]
use bigquery::BigQuerySinkError;

use crate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    table::{TableId, TableSchema},
};

use super::PipelineResumptionState;

#[cfg(feature = "duckdb")]
use self::duckdb::{DuckDbExecutorError, DuckDbRequest};

#[cfg(feature = "bigquery")]
pub mod bigquery;
#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "stdout")]
pub mod stdout;

#[derive(Debug, Error)]
pub enum SinkError {
    #[cfg(feature = "duckdb")]
    #[error("failed to send duckdb request")]
    SendError(#[from] SendError<DuckDbRequest>),

    #[cfg(feature = "duckdb")]
    #[error("duckdb executor error: {0}")]
    DuckDbExecutor(#[from] DuckDbExecutorError),

    #[error("no response received")]
    NoResponseReceived,

    #[cfg(feature = "bigquery")]
    #[error("bigquery sink error: {0}")]
    BigQuerySink(#[from] BigQuerySinkError),

    //TODO: remove and use the one wrapped inside BigQuerySinkError
    #[cfg(feature = "bigquery")]
    #[error("bigquery error: {0}")]
    BigQuery(#[from] BQError),
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

#[async_trait]
pub trait BatchSink {
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, SinkError>;
    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), SinkError>;
    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), SinkError>;
    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, SinkError>;
    async fn table_copied(&mut self, table_id: TableId) -> Result<(), SinkError>;
    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), SinkError>;
}
