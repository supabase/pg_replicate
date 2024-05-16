use std::collections::HashMap;

use postgres_protocol::message::backend::{LogicalReplicationMessage, ReplicationMessage};

use tokio_postgres::binary_copy::BinaryCopyOutRow;

use crate::table::{ColumnSchema, TableId, TableSchema};

pub mod json;
pub mod table_row;

pub trait TryFromTableRow<Err> {
    type Output;

    fn try_from(
        &self,
        row: &BinaryCopyOutRow,
        column_schemas: &[ColumnSchema],
    ) -> Result<Self::Output, Err>;
}

pub trait TryFromReplicationMessage<Err> {
    type Output;

    fn try_from(
        &self,
        message: ReplicationMessage<LogicalReplicationMessage>,
        table_schemas: &HashMap<TableId, TableSchema>,
    ) -> Result<Self::Output, Err>;
}
