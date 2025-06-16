use crate::v2::config::batch::BatchConfig;
use postgres::tokio::config::PgConnectionConfig;

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub pg_database_config: PgConnectionConfig,
    pub batch_config: BatchConfig,
}
