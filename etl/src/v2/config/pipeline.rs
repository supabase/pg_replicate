use crate::v2::config::batch::BatchConfig;
use postgres::tokio::options::PgDatabaseConfig;

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub pg_database_config: PgDatabaseConfig,
    pub batch_config: BatchConfig,
}
