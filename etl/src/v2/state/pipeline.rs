use postgres_protocol::Lsn;

#[derive(Debug)]
pub struct PipelineState {
    /// The last LSN which was applied by a pipeline.
    last_lsn: Lsn,
}
