use crate::v2::pipeline::PipelineId;
use postgres::schema::Oid;
use std::fmt;
use tokio_postgres::types::PgLsn;

#[derive(Debug, Clone)]
pub struct TableReplicationState {
    /// The pipeline id to which this state refers.
    pub pipeline_id: PipelineId,
    /// The table (relation) OID to which this state refers.
    pub table_id: Oid,
    /// The phase of replication of the table.
    pub phase: TableReplicationPhase,
}

impl TableReplicationState {
    pub fn new(pipeline_id: PipelineId, table_id: Oid, phase: TableReplicationPhase) -> Self {
        Self {
            pipeline_id,
            table_id,
            phase,
        }
    }

    pub fn init(pipeline_id: PipelineId, table_id: Oid) -> Self {
        Self::new(pipeline_id, table_id, TableReplicationPhase::Init)
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
    Skipped,
    Unknown,
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
    SyncWait,
    Catchup,
    SyncDone,
    Ready,
    Skipped,
    Unknown,
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
            Self::Unknown => false,
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
            Self::Unknown => true,
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
            TableReplicationPhase::Skipped => Self::Skipped,
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
            Self::Skipped => write!(f, "skipped"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}
