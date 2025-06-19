use postgres::schema::TableId;
use std::fmt;
use tokio_postgres::types::PgLsn;

#[derive(Debug, Clone)]
pub struct TableReplicationState {
    /// The table (relation) OID to which this state refers.
    pub table_id: TableId,

    /// The phase of replication of the table.
    pub phase: TableReplicationPhase,
}

impl TableReplicationState {
    pub fn new(table_id: TableId, phase: TableReplicationPhase) -> Self {
        Self { table_id, phase }
    }

    pub fn init(table_id: TableId) -> Self {
        Self::new(table_id, TableReplicationPhase::DataSync)
    }

    pub fn with_phase(self, phase: TableReplicationPhase) -> TableReplicationState {
        TableReplicationState { phase, ..self }
    }
}

impl PartialEq for TableReplicationState {
    fn eq(&self, other: &Self) -> bool {
        self.table_id == other.table_id
    }
}

impl Eq for TableReplicationState {}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TableReplicationPhase {
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
    Skipped,
}

impl TableReplicationPhase {
    pub fn as_type(&self) -> TableReplicationPhaseType {
        self.into()
    }
}

// TODO: we may not need as many phases as we have now.
// Evaluate this once the code is more mature.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TableReplicationPhaseType {
    DataSync,
    FinishedCopy,
    SyncWait,
    Catchup,
    SyncDone,
    Ready,
    Skipped,
}

impl TableReplicationPhaseType {
    /// Returns `true` if the phase should be saved into the state store, `false` otherwise.
    pub fn should_store(&self) -> bool {
        match self {
            Self::DataSync => true,
            Self::FinishedCopy => true,
            Self::SyncWait => false,
            Self::Catchup => false,
            Self::SyncDone => true,
            Self::Ready => true,
            Self::Skipped => true,
        }
    }

    /// Returns `true` if a table with this phase is done processing, `false` otherwise.
    ///
    /// A table is done processing, when its events are being processed by the apply worker instead
    /// of a table sync worker.
    pub fn is_done(&self) -> bool {
        match self {
            Self::DataSync => false,
            Self::FinishedCopy => false,
            Self::SyncWait => false,
            Self::Catchup => false,
            Self::SyncDone => false,
            Self::Ready => true,
            Self::Skipped => true,
        }
    }
}

impl<'a> From<&'a TableReplicationPhase> for TableReplicationPhaseType {
    fn from(phase: &'a TableReplicationPhase) -> Self {
        match phase {
            TableReplicationPhase::DataSync => Self::DataSync,
            TableReplicationPhase::FinishedCopy => Self::FinishedCopy,
            TableReplicationPhase::SyncWait => Self::SyncWait,
            TableReplicationPhase::Catchup { .. } => Self::Catchup,
            TableReplicationPhase::SyncDone { .. } => Self::SyncDone,
            TableReplicationPhase::Ready => Self::Ready,
            TableReplicationPhase::Skipped => Self::Skipped,
        }
    }
}

impl fmt::Display for TableReplicationPhaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DataSync => write!(f, "data_sync"),
            Self::FinishedCopy => write!(f, "finished_copy"),
            Self::SyncWait => write!(f, "sync_wait"),
            Self::Catchup => write!(f, "catchup"),
            Self::SyncDone => write!(f, "sync_done"),
            Self::Ready => write!(f, "ready"),
            Self::Skipped => write!(f, "skipped"),
        }
    }
}
