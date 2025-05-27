use tokio_postgres::types::PgLsn;

#[derive(Debug)]
pub struct PipelineState {
    /// The last LSN which was applied by a pipeline.
    pub last_lsn: PgLsn,
}

impl Default for PipelineState {
    fn default() -> Self {
        Self {
            last_lsn: PgLsn::from(0),
        }
    }
}
