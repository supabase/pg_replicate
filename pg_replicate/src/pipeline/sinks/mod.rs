use std::collections::HashMap;

use async_trait::async_trait;
use thiserror::Error;
use tokio_postgres::types::PgLsn;

use crate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    table::{TableId, TableSchema},
};

use super::PipelineResumptionState;

#[cfg(feature = "bigquery")]
pub mod bigquery;
#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "stdout")]
pub mod stdout;

pub trait SinkError: std::error::Error + Send + Sync + 'static {}

#[derive(Debug, Error)]
#[error("unreachable")]
pub enum InfallibleSinkError {}
impl SinkError for InfallibleSinkError {}

#[async_trait]
pub trait Sink {
    type Error: SinkError;
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error>;
    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error>;
    async fn write_table_row(
        &mut self,
        row: TableRow,
        table_id: TableId,
    ) -> Result<(), Self::Error>;
    async fn write_cdc_event(&mut self, event: CdcEvent) -> Result<PgLsn, Self::Error>;
    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error>;
    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error>;
}

#[async_trait]
pub trait BatchSink {
    type Error: SinkError;
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error>;
    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error>;
    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error>;
    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error>;
    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error>;
    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error>;
}
