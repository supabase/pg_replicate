use std::collections::HashSet;

use destinations::DestinationError;
use postgres::schema::TableId;
use sources::SourceError;
use thiserror::Error;
use tokio_postgres::types::PgLsn;

pub mod batching;
pub mod destinations;
pub mod sources;

#[derive(Debug)]
pub enum PipelineAction {
    TableCopiesOnly,
    CdcOnly,
    Both,
}

#[derive(Debug)]
pub struct PipelineResumptionState {
    pub copied_tables: HashSet<TableId>,
    pub last_lsn: PgLsn,
}

#[derive(Debug, Error)]
pub enum PipelineError<SrcErr: SourceError, DstErr: DestinationError> {
    #[error("source error: {0}")]
    Source(#[source] SrcErr),

    #[error("destination error: {0}")]
    Destination(#[source] DstErr),

    #[error("source error: {0}")]
    CommonSource(#[from] sources::CommonSourceError),
}
