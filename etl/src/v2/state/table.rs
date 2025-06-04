use postgres::schema::Oid;
use std::fmt;
use tokio_postgres::types::PgLsn;

#[derive(Debug, Clone)]
pub struct TableReplicationState {
    /// The table (relation) OID to which this subscription refers.
    pub id: Oid,
    /// The phase of replication of the table.
    pub phase: TableReplicationPhase,
}

impl TableReplicationState {
    pub fn new(id: Oid, phase: TableReplicationPhase) -> Self {
        Self { id, phase }
    }

    pub fn init(id: Oid) -> Self {
        Self::new(id, TableReplicationPhase::Init)
    }

    pub fn with_phase(self, phase: TableReplicationPhase) -> TableReplicationState {
        TableReplicationState { phase, ..self }
    }
}

impl PartialEq for TableReplicationState {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
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
    Ready {
        /// The LSN of the apply worker which set this state to ready.
        lsn: PgLsn,
    },
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
        // TODO: we might want to statically enforce the two different phase type groups.
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
            TableReplicationPhase::Ready { .. } => Self::Ready,
            TableReplicationPhase::Unknown => Self::Unknown,
        }
    }
}

impl fmt::Display for TableReplicationPhaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Init => write!(f, "init"),
            Self::DataSync => write!(f, "data_sync"),
            Self::FinishedCopy => write!(f, "finished_copy"),
            Self::SyncWait => write!(f, "sync_wait"),
            Self::Catchup => write!(f, "catchup"),
            Self::SyncDone => write!(f, "sync_done"),
            Self::Ready => write!(f, "ready"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}
