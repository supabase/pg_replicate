use std::borrow::Borrow;
use tokio_postgres::types::PgLsn;

#[derive(Debug, Clone)]
pub struct PipelineState {
    /// The identifier of the pipeline.
    pub id: u64,
    /// The last LSN which was applied by a pipeline.
    pub last_lsn: PgLsn,
}

impl PartialEq for PipelineState {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for PipelineState {}

impl Borrow<u64> for PipelineState {
    fn borrow(&self) -> &u64 {
        &self.id
    }
}
