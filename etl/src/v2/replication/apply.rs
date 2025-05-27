use crate::v2::destination::Destination;
use crate::v2::replication::base::SyncingTables;
use crate::v2::state::store::base::PipelineStateStore;

#[derive(Debug)]
pub struct ApplyJob<T> {
    syncing_tables: T,
}

impl<T> ApplyJob<T>
where
    T: SyncingTables,
{
    pub fn new(syncing_tables: T) -> Self {
        Self { syncing_tables }
    }

    pub async fn start_loop<S, D>(&self)
    where
        S: PipelineStateStore,
        D: Destination,
    {
        loop {
            // Read from socket until end or until boundary. (Modify custom cdcstream if needed)

            // For each operation apply it and call the table syncing.
            
            // 
        }
    }
}
