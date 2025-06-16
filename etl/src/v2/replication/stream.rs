use futures::{ready, Stream};
use pin_project_lite::pin_project;
use postgres::schema::ColumnSchema;
use postgres::time::POSTGRES_EPOCH;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use postgres_replication::LogicalReplicationStream;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTimeError;
use thiserror::Error;
use tokio_postgres::types::PgLsn;
use tokio_postgres::CopyOutStream;

use crate::conversions::table_row::{TableRow, TableRowConversionError, TableRowConverter};

/// Errors that can occur while streaming table copy data.
#[derive(Debug, Error)]
pub enum TableCopyStreamError {
    /// An error occurred when copying table data from the stream.
    #[error("An error occurred when copying table data from the stream: {0}")]
    TableCopyFailed(#[from] tokio_postgres::Error),

    /// An error occurred while converting a table row during table copy.
    #[error("An error occurred while converting a table row during table copy: {0}")]
    Conversion(#[from] TableRowConversionError),
}

pin_project! {
    /// A stream that yields rows from a PostgreSQL COPY operation.
    ///
    /// This stream wraps a [`CopyOutStream`] and converts each row into a [`TableRow`]
    /// using the provided column schemas. The conversion process handles both text and
    /// binary format data.
    #[must_use = "streams do nothing unless polled"]
    pub struct TableCopyStream<'a> {
        #[pin]
        stream: CopyOutStream,
        column_schemas: &'a [ColumnSchema],
    }
}

impl<'a> TableCopyStream<'a> {
    /// Creates a new [`TableCopyStream`] from a [`CopyOutStream`] and column schemas.
    ///
    /// The column schemas are used to convert the raw PostgreSQL data into [`TableRow`]s.
    pub fn wrap(stream: CopyOutStream, column_schemas: &'a [ColumnSchema]) -> Self {
        Self {
            stream,
            column_schemas,
        }
    }
}

impl<'a> Stream for TableCopyStream<'a> {
    type Item = Result<TableRow, TableCopyStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match ready!(this.stream.poll_next(cx)) {
            // TODO: allow pluggable table row conversion based on if the data is in text or binary format.
            Some(Ok(row)) => match TableRowConverter::try_from(&row, this.column_schemas) {
                Ok(row) => Poll::Ready(Some(Ok(row))),
                Err(err) => Poll::Ready(Some(Err(err.into()))),
            },
            Some(Err(err)) => Poll::Ready(Some(Err(err.into()))),
            None => Poll::Ready(None),
        }
    }
}

/// Errors that can occur while streaming logical replication events.
#[derive(Debug, Error)]
pub enum EventsStreamError {
    /// An error occurred when copying table data from the stream.
    #[error("An error occurred when copying table data from the stream: {0}")]
    TableCopyFailed(#[from] tokio_postgres::Error),

    /// An error occurred while calculating the elapsed time since PostgreSQL epoch.
    #[error("An error occurred while determining the elapsed time: {0}")]
    EpochCalculationFailed(#[from] SystemTimeError),
}

pin_project! {
    pub struct EventsStream {
        #[pin]
        stream: LogicalReplicationStream,
    }
}

impl EventsStream {
    /// Creates a new [`EventsStream`] from a [`LogicalReplicationStream`].
    pub fn wrap(stream: LogicalReplicationStream) -> Self {
        Self { stream }
    }

    /// Sends a status update to the PostgreSQL server with the current replication position.
    ///
    /// The timestamp is calculated relative to the PostgreSQL epoch (2000-01-01 00:00:00 UTC).
    /// This is used to inform the server about the client's progress in processing replication events.
    pub async fn send_status_update(
        self: Pin<&mut Self>,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        apply_lsn: PgLsn,
    ) -> Result<(), EventsStreamError> {
        let this = self.project();
        // The client's system clock at the time of transmission, as microseconds since midnight
        // on 2000-01-01.
        //
        // Reference: https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-STANDBY-STATUS-UPDATE
        let ts = POSTGRES_EPOCH.elapsed()?.as_micros() as i64;

        this.stream
            .standby_status_update(write_lsn, flush_lsn, apply_lsn, ts, 0)
            .await?;

        Ok(())
    }
}

impl Stream for EventsStream {
    type Item = Result<ReplicationMessage<LogicalReplicationMessage>, EventsStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
