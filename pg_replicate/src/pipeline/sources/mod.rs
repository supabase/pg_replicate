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

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("source error: {0}")]
    Postgres(#[from] PostgresSourceError),

    #[error("table copy stream error: {0}")]
    TableCopyStream(#[from] TableCopyStreamError),

    #[error("cdc stream error: {0}")]
    CdcStream(#[from] CdcStreamError),

    #[error("status update error: {0}")]
    StatusUpdate(#[from] StatusUpdateError),
}

#[async_trait]
pub trait Source {
    fn get_table_schemas(&self) -> &HashMap<TableId, TableSchema>;

    async fn get_table_copy_stream(
        &self,
        table_name: &TableName,
        column_schemas: &[ColumnSchema],
    ) -> Result<TableCopyStream, SourceError>;

    async fn commit_transaction(&self) -> Result<(), SourceError>;

    async fn get_cdc_stream(&self, start_lsn: PgLsn) -> Result<CdcStream, SourceError>;
}
