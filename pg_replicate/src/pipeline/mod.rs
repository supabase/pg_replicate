use std::collections::HashSet;

use thiserror::Error;
use tokio_postgres::types::PgLsn;

use crate::table::TableId;

use self::{sinks::SinkError, sources::SourceError};

pub mod batching;
pub mod data_pipeline;
pub mod sinks;
pub mod sources;

#[derive(Debug)]
pub enum PipelineAction {
    TableCopiesOnly,
    CdcOnly,
    Both,
}

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("source error: {0}")]
    SourceError(#[from] SourceError),

    #[error("sink error: {0}")]
    SinkError(#[from] SinkError),
}

pub struct PipelineResumptionState {
    pub copied_tables: HashSet<TableId>,
    pub last_lsn: PgLsn,
}
