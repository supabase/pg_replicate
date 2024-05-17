use std::collections::HashMap;

use postgres_protocol::message::backend::{LogicalReplicationMessage, ReplicationMessage};

use crate::table::{TableId, TableSchema};

pub mod json;
pub mod table_row;

pub trait TryFromReplicationMessage<Err> {
    type Output;

    fn try_from(
        &self,
        message: ReplicationMessage<LogicalReplicationMessage>,
        table_schemas: &HashMap<TableId, TableSchema>,
    ) -> Result<Self::Output, Err>;
}
