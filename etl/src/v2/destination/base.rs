use postgres::schema::{Oid, TableSchema};
use std::future::Future;
use thiserror::Error;

use crate::conversions::table_row::TableRow;
use crate::v2::conversions::event::Event;

#[derive(Debug, Error)]
pub enum DestinationError {}

pub trait Destination {
    fn write_table_schema(
        &self,
        schema: TableSchema,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send;

    fn copy_table_rows(
        &self,
        id: Oid,
        rows: Vec<TableRow>,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send;

    fn apply_event(
        &self,
        events: Event,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send;

    fn apply_events(
        &self,
        events: Vec<Event>,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send;
}
