use crate::conversions::cdc_event::CdcEvent;
use crate::conversions::table_row::TableRow;
use postgres::schema::{Oid, TableSchema};
use std::future::Future;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DestinationError {}

// TODO: migrate conversions into v2.
pub trait Destination {
    fn write_table_schema(
        &self,
        schema: TableSchema,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send;

    fn copy_table(
        &self,
        id: Oid,
        rows: Vec<TableRow>,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send;

    fn apply_events(
        &self,
        events: Vec<CdcEvent>,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send;
}
