use async_trait::async_trait;
use thiserror::Error;

use crate::conversions::table_row::TableRow;

pub mod duckdb;
pub mod stdout;

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("some error")]
    SomeError,
}

#[async_trait]
pub trait Sink<CRO: Send + Sync> {
    async fn write_table_row(&self, row: TableRow) -> Result<(), SinkError>;
    async fn write_cdc_event(&self, event: CRO) -> Result<(), SinkError>;
}
