use crate::v2::config::batch::BatchConfig;
use crate::v2::config::retry::RetryConfig;
use postgres::tokio::config::PgConnectionConfig;

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub pg_connection_config: PgConnectionConfig,
    pub batch_config: BatchConfig,
    pub retry_config: RetryConfig,
}
