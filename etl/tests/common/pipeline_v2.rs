use etl::v2::config::batch::BatchConfig;
use etl::v2::config::pipeline::PipelineConfig;
use etl::v2::config::retry::RetryConfig;
use etl::v2::destination::base::Destination;
use etl::v2::pipeline::{Pipeline, PipelineIdentity};
use etl::v2::state::store::base::StateStore;
use postgres::tokio::options::PgDatabaseConfig;

pub fn spawn_pg_pipeline<S, D>(
    publication_name: &str,
    pg_database_config: &PgDatabaseConfig,
    state_store: S,
    destination: D,
) -> Pipeline<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    let identify = PipelineIdentity::new(0, publication_name);
    let config = PipelineConfig {
        pg_database_config: pg_database_config.clone(),
        batch_config: BatchConfig::default(),
        retry_config: RetryConfig::default(),
    };

    Pipeline::new(identify, config, vec![], state_store, destination)
}
