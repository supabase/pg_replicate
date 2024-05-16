use std::collections::HashMap;

use async_trait::async_trait;
use thiserror::Error;
use tokio_postgres::types::PgLsn;

use crate::{
    conversions::{TryFromReplicationMessage, TryFromTableRow},
    table::{ColumnSchema, TableId, TableName, TableSchema},
};

use self::postgres::{CdcStream, PostgresSourceError, TableCopyStream, TableCopyStreamError};

pub mod postgres;

#[derive(Debug, Error)]
pub enum SourceError<TE> {
    #[error("source error: {0}")]
    Postgres(#[from] PostgresSourceError),

    #[error("table copy stream error: {0}")]
    TableCopyStream(#[from] TableCopyStreamError<TE>),
}

#[async_trait]
pub trait Source<
    'a,
    'b,
    TE,
    TR: TryFromTableRow<TE> + Sync + Send,
    RE,
    RM: TryFromReplicationMessage<RE> + Sync + Send,
>
{
    fn get_table_schemas(&self) -> &HashMap<TableId, TableSchema>;

    async fn get_table_copy_stream(
        &self,
        table_name: &TableName,
        column_schemas: &'a [ColumnSchema],
        converter: &'a TR,
    ) -> Result<TableCopyStream<'a, TR, TE>, SourceError<TE>>;

    async fn commit_transaction(&self) -> Result<(), SourceError<TE>>;

    async fn get_cdc_stream(
        &'b self,
        start_lsn: PgLsn,
        converter: &'a RM,
    ) -> Result<CdcStream<'a, 'b, RM, RE>, SourceError<TE>>;
}
