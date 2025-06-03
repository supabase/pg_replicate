use etl::v2::config::pipeline::PipelineConfig;
use etl::v2::destination::base::Destination;
use etl::v2::pipeline::{Pipeline, PipelineIdentity};
use etl::v2::state::store::base::StateStore;
use postgres::tokio::options::PgDatabaseOptions;

pub fn spawn_pg_pipeline<S, D>(
    publication_name: &str,
    options: &PgDatabaseOptions,
    state_store: S,
    destination: D,
) -> Pipeline<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    let identify = PipelineIdentity::new(0, publication_name);
    let config = PipelineConfig::default();

    let pipeline = Pipeline::new(
        identify,
        config,
        options.clone(),
        vec![],
        state_store,
        destination,
    );

    pipeline
}
