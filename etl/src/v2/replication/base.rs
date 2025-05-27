use std::future::Future;
use tokio_postgres::types::PgLsn;

pub trait SyncingTables {
    fn process_syncing_tables(current_lsn: PgLsn) -> impl Future<Output = ()> + Send;
}
