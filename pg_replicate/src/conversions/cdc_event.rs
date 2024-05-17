use postgres_protocol::message::backend::{
    BeginBody, CommitBody, DeleteBody, InsertBody, LogicalReplicationMessage, RelationBody,
    ReplicationMessage, UpdateBody,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CdcEventConversionError {
    #[error("message not supported")]
    MessageNotSupported,

    #[error("unknown replication message")]
    UnknownReplicationMessage,
}

impl TryFrom<ReplicationMessage<LogicalReplicationMessage>> for CdcEvent {
    type Error = CdcEventConversionError;

    fn try_from(value: ReplicationMessage<LogicalReplicationMessage>) -> Result<Self, Self::Error> {
        match value {
            ReplicationMessage::XLogData(xlog_data) => match xlog_data.into_data() {
                LogicalReplicationMessage::Begin(begin_body) => Ok(CdcEvent::Begin(begin_body)),
                LogicalReplicationMessage::Commit(commit_body) => Ok(CdcEvent::Commit(commit_body)),
                LogicalReplicationMessage::Origin(_) => {
                    Err(CdcEventConversionError::MessageNotSupported)
                }
                LogicalReplicationMessage::Relation(relation_body) => {
                    Ok(CdcEvent::Relation(relation_body))
                }
                LogicalReplicationMessage::Type(_) => {
                    Err(CdcEventConversionError::MessageNotSupported)
                }
                LogicalReplicationMessage::Insert(insert_body) => Ok(CdcEvent::Insert(insert_body)),
                LogicalReplicationMessage::Update(update_body) => Ok(CdcEvent::Update(update_body)),
                LogicalReplicationMessage::Delete(delete_body) => Ok(CdcEvent::Delete(delete_body)),
                LogicalReplicationMessage::Truncate(_) => {
                    Err(CdcEventConversionError::MessageNotSupported)
                }
                _ => Err(CdcEventConversionError::UnknownReplicationMessage),
            },
            ReplicationMessage::PrimaryKeepAlive(keep_alive) => Ok(CdcEvent::KeepAliveRequested {
                reply: keep_alive.reply() == 1,
            }),
            _ => Err(CdcEventConversionError::UnknownReplicationMessage),
        }
    }
}

#[derive(Debug)]
pub enum CdcEvent {
    Begin(BeginBody),
    Commit(CommitBody),
    Insert(InsertBody),
    Update(UpdateBody),
    Delete(DeleteBody),
    Relation(RelationBody),
    KeepAliveRequested { reply: bool },
}
