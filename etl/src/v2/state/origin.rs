use crate::v2::pipeline::PipelineId;
use postgres::schema::Oid;
use tokio_postgres::types::PgLsn;

#[derive(Debug, Clone)]
pub struct ReplicationOriginState {
    /// The pipeline to which this origin state belongs.
    pub pipeline_id: PipelineId,
    /// The table to which this origin state belongs.
    ///
    /// If no table id is set, it means that this origin tracks all tables.
    pub table_id: Option<Oid>,
    /// Location of the latest commit from the remote side which will be used as starting point for
    /// the CDC stream within the apply loop.
    pub remote_lsn: PgLsn,
}

impl ReplicationOriginState {
    pub fn new(pipeline_id: PipelineId, table_id: Option<Oid>, remote_lsn: PgLsn) -> Self {
        Self {
            pipeline_id,
            table_id,
            remote_lsn,
        }
    }
}
