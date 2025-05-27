use postgres::schema::Oid;
use std::borrow::Borrow;
use tokio_postgres::types::PgLsn;

#[derive(Debug)]
pub struct TableReplicationState {
    /// The table (relation) OID to which this subscription refers.
    pub id: Oid,
    /// The phase of replication of the table.
    pub phase: TableReplicationPhase,
}

impl PartialEq for TableReplicationState {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Borrow<Oid> for TableReplicationState {
    fn borrow(&self) -> &Oid {
        &self.id
    }
}

impl Eq for TableReplicationState {}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TableReplicationPhase {
    Init,
    DataSync,
    FinishedCopy,
    SyncWait,
    Catchup {
        /// The LSN to catch up to.
        lsn: PgLsn,
    },
    SyncDone {
        /// The LSN up to which the table sync arrived.
        lsn: PgLsn,
    },
    Ready,
    Unknown,
}

impl TableReplicationPhase {
    pub fn as_type(&self) -> TableReplicationPhaseType {
        self.into()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TableReplicationPhaseType {
    Init,
    DataSync,
    FinishedCopy,
    SyncWait,
    Catchup,
    SyncDone,
    Ready,
    Unknown,
}

impl TableReplicationPhaseType {
    pub fn should_store(&self) -> bool {
        match self {
            Self::Init => true,
            Self::DataSync => true,
            Self::FinishedCopy => true,
            Self::SyncDone => true,
            Self::Ready => true,
            // We set `false` to the statuses which are exclusively used for cross-task synchronization
            // and do not need to be stored.
            Self::SyncWait => false,
            Self::Catchup => false,
            Self::Unknown => false,
        }
    }
}

impl<'a> From<&'a TableReplicationPhase> for TableReplicationPhaseType {
    fn from(phase: &'a TableReplicationPhase) -> Self {
        match phase {
            TableReplicationPhase::Init => Self::Init,
            TableReplicationPhase::DataSync => Self::DataSync,
            TableReplicationPhase::FinishedCopy => Self::FinishedCopy,
            TableReplicationPhase::SyncWait => Self::SyncWait,
            TableReplicationPhase::Catchup { .. } => Self::Catchup,
            TableReplicationPhase::SyncDone { .. } => Self::SyncDone,
            TableReplicationPhase::Ready => Self::Ready,
            TableReplicationPhase::Unknown => Self::Unknown,
        }
    }
}
