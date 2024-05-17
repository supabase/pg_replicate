use std::collections::HashMap;

use async_trait::async_trait;
use thiserror::Error;
use tokio_postgres::types::PgLsn;

use crate::table::{ColumnSchema, TableId, TableName, TableSchema};

use self::postgres::{
    CdcStream, CdcStreamError, PostgresSourceError, TableCopyStream, TableCopyStreamError,
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
}

#[async_trait]
pub trait Source<'a> {
    fn get_table_schemas(&self) -> &HashMap<TableId, TableSchema>;

    async fn get_table_copy_stream(
        &'a self,
        table_name: &TableName,
        column_schemas: &'a [ColumnSchema],
    ) -> Result<TableCopyStream<'a>, SourceError>;

    async fn commit_transaction(&self) -> Result<(), SourceError>;

    async fn get_cdc_stream(&'a self, start_lsn: PgLsn) -> Result<CdcStream<'a>, SourceError>;
}
