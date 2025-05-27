use postgres::tokio::options::PgDatabaseOptions;

#[derive(Debug)]
pub struct PgReplicationClient {}

impl PgReplicationClient {
    pub fn new(options: PgDatabaseOptions) -> Self {
        Self {}
    }
}
