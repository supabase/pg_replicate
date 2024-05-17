use std::collections::HashMap;

use async_trait::async_trait;
use thiserror::Error;

use crate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    table::TableSchema,
};

pub mod duckdb;
pub mod stdout;

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("some error")]
    SomeError,
}

#[async_trait]
pub trait Sink {
    async fn write_table_schemas(
        &self,
        table_schemas: &HashMap<u32, TableSchema>,
    ) -> Result<(), SinkError>;
    async fn write_table_row(&self, row: TableRow) -> Result<(), SinkError>;
    async fn write_cdc_event(&self, event: CdcEvent) -> Result<(), SinkError>;
}
