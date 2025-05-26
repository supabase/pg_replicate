use postgres::schema::Oid;
use tokio_postgres::types::PgLsn;

#[derive(Debug)]
pub struct RelationSubscriptionState {
    /// The relation (table) OID to which this subscription refers.
    rel_id: Oid,
    /// The status of the subscription bound to a relation.
    status: RelationSubscriptionStatus,
}

#[derive(Debug)]
pub enum RelationSubscriptionStatus {
    Init,
    DataSync,
    FinishedCopy,
    SyncWait,
    Catchup,
    SyncDone {
        /// The LSN up to which the table sync arrived.
        lsn: PgLsn,
    },
    Ready,
    Unknown,
}

impl RelationSubscriptionStatus {
    pub fn should_store(&self) -> bool {
        match self {
            RelationSubscriptionStatus::Init => true,
            RelationSubscriptionStatus::DataSync => true,
            RelationSubscriptionStatus::FinishedCopy => true,
            RelationSubscriptionStatus::SyncDone { .. } => true,
            RelationSubscriptionStatus::Ready => true,
            // We set `false` to the statuses which are exclusively used for cross-task synchronization
            // and do not need to be stored.
            RelationSubscriptionStatus::SyncWait => false,
            RelationSubscriptionStatus::Catchup => false,
            RelationSubscriptionStatus::Unknown => false,
        }
    }
}
