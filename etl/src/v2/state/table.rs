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
        Self::new(table_id, TableReplicationPhase::Init)
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
    /// Set when the pipeline first starts and encounters a table for the first time
    Init,

    /// Set when initial table copy is being performed
    DataSync,

    /// Set when initial table copy is done
    FinishedCopy,

    /// Set when waiting for the apply worker to pause
    SyncWait,

    /// Set by the apply worker when it is paused
    Catchup {
        /// The lsn to catch up to. This is the location where the apply loop is paused
        lsn: PgLsn,
    },

    /// Set by the table-sync worker in the catch-up phase when catch up
    /// phase is completed and table-sync worker has caught up with the
    /// apply worker's lsn position
    SyncDone {
        /// The lsn up to which the table sync worker has caught up
        lsn: PgLsn,
    },

    /// Set by apply worker when it has caught up with the table-sync worker's
    /// catch up lsn position
    Ready,

    /// Set when a table is no longer being synced because of an error
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
    Init,
    DataSync,
    FinishedCopy,
    SyncWait, // in-memory
    Catchup,  // in-memory
    SyncDone, //in-memory
    Ready,
    Skipped,
}

impl TableReplicationPhaseType {
    /// Returns `true` if the phase should be saved into the state store, `false` otherwise.
    pub fn should_store(&self) -> bool {
        match self {
            Self::Init => true,
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
            Self::Init => false,
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
            TableReplicationPhase::Init => Self::Init,
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
            Self::Init => write!(f, "init"),
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
