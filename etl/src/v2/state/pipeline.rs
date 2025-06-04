use tokio_postgres::types::PgLsn;

#[derive(Debug, Clone)]
pub struct PipelineState {
    /// The identifier of the pipeline.
    pub id: u64,
    /// The last LSN which was applied by a pipeline.
    pub last_lsn: PgLsn,
}

impl PipelineState {
    pub fn init(id: u64) -> Self {
        Self {
            id,
            last_lsn: PgLsn::from(0),
        }
    }
}

impl PartialEq for PipelineState {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for PipelineState {}
