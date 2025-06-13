use crate::v2::config::batch::BatchConfig;
use crate::v2::config::retry::RetryConfig;
use postgres::tokio::options::PgDatabaseConfig;

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub pg_database_config: PgDatabaseConfig,
    pub batch_config: BatchConfig,
    pub retry_config: RetryConfig,
}
