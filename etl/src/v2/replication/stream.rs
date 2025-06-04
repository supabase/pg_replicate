use crate::conversions::table_row::{TableRow, TableRowConversionError, TableRowConverter};
use futures::{ready, Stream};
use pin_project_lite::pin_project;
use postgres::schema::ColumnSchema;
use std::pin::Pin;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio_postgres::CopyOutStream;

#[derive(Debug, Error)]
pub enum TableCopyStreamError {
    #[error("An error occurred when copying table data from the stream: {0}")]
    TokioPostgres(#[from] tokio_postgres::Error),

    #[error("An error occurred while converting a table row during table copy: {0}")]
    Conversion(#[from] TableRowConversionError),
}

pin_project! {
    #[must_use = "streams do nothing unless polled"]
    pub struct TableCopyStream<'a> {
        #[pin]
        stream: CopyOutStream,
        column_schemas: &'a [ColumnSchema],
    }
}

impl<'a> TableCopyStream<'a> {
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
