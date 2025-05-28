use postgres::tokio::options::PgDatabaseOptions;

// TODO: implement the actual client.
#[derive(Debug)]
pub struct PgReplicationClient {}

impl PgReplicationClient {
    pub fn new(options: PgDatabaseOptions) -> Self {
        Self {}
    }
}
