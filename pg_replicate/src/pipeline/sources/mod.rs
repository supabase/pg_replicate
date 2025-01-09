use std::collections::HashMap;

use async_trait::async_trait;
use thiserror::Error;
use tokio_postgres::types::PgLsn;

use crate::table::{ColumnSchema, TableId, TableName, TableSchema};

use self::postgres::{
    CdcStream, CdcStreamError, PostgresSourceError, StatusUpdateError, TableCopyStream,
    TableCopyStreamError,
};

pub mod postgres;

pub trait SourceError: std::error::Error + Send + Sync + 'static {}

#[derive(Debug, Error)]
#[error("unreachable")]
pub enum InfallibleSourceError {}
impl SourceError for InfallibleSourceError {}

#[derive(Debug, Error)]
pub enum CommonSourceError {
    #[error("source error: {0}")]
    Postgres(#[from] PostgresSourceError),

    #[error("table copy stream error: {0}")]
    TableCopyStream(#[from] TableCopyStreamError),

    #[error("cdc stream error: {0}")]
    CdcStream(#[from] CdcStreamError),

    #[error("status update error: {0}")]
    StatusUpdate(#[from] StatusUpdateError),
}

impl SourceError for CommonSourceError {}

#[async_trait]
pub trait Source {
    type Error: SourceError;

    fn get_table_schemas(&self) -> &HashMap<TableId, TableSchema>;

    async fn get_table_copy_stream(
        &self,
        table_name: &TableName,
        column_schemas: &[ColumnSchema],
    ) -> Result<TableCopyStream, Self::Error>;

    async fn commit_transaction(&mut self) -> Result<(), Self::Error>;

    async fn get_cdc_stream(&self, start_lsn: PgLsn) -> Result<CdcStream, Self::Error>;
}
