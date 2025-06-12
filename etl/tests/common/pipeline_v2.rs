use etl::v2::config::batch::BatchConfig;
use etl::v2::config::pipeline::PipelineConfig;
use etl::v2::config::retry::RetryConfig;
use etl::v2::destination::base::Destination;
use etl::v2::pipeline::{Pipeline, PipelineIdentity};
use etl::v2::state::store::base::StateStore;
use postgres::tokio::options::PgDatabaseConfig;
use rand::random;
use std::time::Duration;

pub fn create_pipeline_identity(publication_name: &str) -> PipelineIdentity {
    let pipeline_id = random();
    PipelineIdentity::new(pipeline_id, publication_name)
}

pub fn spawn_pg_pipeline<S, D>(
    identity: &PipelineIdentity,
    pg_database_config: &PgDatabaseConfig,
    state_store: S,
    destination: D,
) -> Pipeline<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    let config = PipelineConfig {
        pg_database_config: pg_database_config.clone(),
        batch_config: BatchConfig {
            max_batch_size: 1,
            max_batch_fill_time: Duration::from_secs(1),
        },
        retry_config: RetryConfig::default(),
    };

    Pipeline::new(identity.clone(), config, vec![], state_store, destination)
}
