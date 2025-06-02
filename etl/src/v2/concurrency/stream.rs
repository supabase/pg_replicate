use core::pin::Pin;
use core::task::{Context, Poll};
use futures::{ready, Future, Stream};
use pin_project_lite::pin_project;
use tokio::sync::futures::Notified;
use tokio::time::{sleep, Sleep};
use tracing::info;

use crate::v2::config::batch::BatchConfig;

/// A trait that determines whether an item in a stream marks the end of a batch.
///
/// Types implementing this trait can signal when they should be the last item in a batch,
/// allowing for intelligent batch boundary decisions in streaming operations.
pub trait BatchBoundary: Sized {
    /// Returns `true` if this item should be the last in its batch, that is, the item is on
    /// a boundary.
    ///
    /// This method is used by [`BoundedBatchStream`] to determine when to emit a batch.
    fn is_on_boundary(&self) -> bool;
}

impl<T: BatchBoundary, E> BatchBoundary for Result<T, E> {
    fn is_on_boundary(&self) -> bool {
        match self {
            Ok(v) => v.is_on_boundary(),
            // We return true since in case of error we want to fail fast, since the batch is
            // anyway going to fail.
            Err(_) => true,
        }
    }
}

// Implementation adapted from:
//  https://github.com/tokio-rs/tokio/blob/master/tokio-stream/src/stream_ext/chunks_timeout.rs
pin_project! {
    /// A stream adapter that batches items based on size limits and timeouts.
    ///
    /// This stream collects items from the underlying stream into batches, emitting them when either:
    /// - The batch reaches its maximum size and the last item indicates it's a batch boundary
    /// - A timeout occurs and the last item indicates it's a batch boundary
    /// - The stream is forcefully stopped
    ///
    /// The stream guarantees that batches will end on items that return `true` from
    /// [`BatchBoundary::is_on_boundary`], unless the stream is forcefully stopped.
    ///
    /// # Implementation Details
    ///
    /// The implementation is adapted from Tokio's chunks_timeout stream extension.
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
    pub struct BoundedBatchStream<'a, B: BatchBoundary, S: Stream<Item = B>> {
        #[pin]
        stream: S,
        #[pin]
        deadline: Option<Sleep>,
        #[pin]
        stream_stop: Option<Notified<'a>>,
        items: Vec<S::Item>,
        batch_config: BatchConfig,
        reset_timer: bool,
        inner_stream_ended: bool,
        stream_stopped: bool
    }
}

impl<'a, B: BatchBoundary, S: Stream<Item = B>> BoundedBatchStream<'a, B, S> {
    /// Creates a new [`BoundedBatchStream`] with the given configuration.
    ///
    /// The stream will batch items according to the provided `batch_config` and can be
    /// stopped using the `stream_stop` notification handle.
    pub fn wrap(stream: S, batch_config: BatchConfig, stream_stop: Option<Notified<'a>>) -> Self {
        BoundedBatchStream {
            stream,
            deadline: None,
            stream_stop,
            items: Vec::with_capacity(batch_config.max_batch_size),
            batch_config,
            reset_timer: true,
            inner_stream_ended: false,
            stream_stopped: false,
        }
    }

    /// Returns a mutable reference to the underlying stream.
    ///
    /// This allows for direct manipulation of the inner stream if needed.
    pub fn get_inner_mut(&mut self) -> &mut S {
        &mut self.stream
    }
}

impl<'a, B: BatchBoundary, S: Stream<Item = B>> Stream for BoundedBatchStream<'a, B, S> {
    type Item = Vec<S::Item>;

    /// Polls the stream for the next batch of items.
    ///
    /// Returns:
    /// - `Poll::Ready(Some(batch))` when a complete batch is available
    /// - `Poll::Ready(None)` when the stream has ended
    /// - `Poll::Pending` when more items are needed to form a batch
    ///
    /// The stream will emit a batch when:
    /// - The batch reaches maximum size and the last item is a batch boundary
    /// - A timeout occurs and the last item is a batch boundary
    /// - The stream is forcefully stopped
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        if *this.inner_stream_ended {
            return Poll::Ready(None);
        }

        loop {
            if *this.stream_stopped {
                return Poll::Ready(None);
            }

            // If the stream has been asked to stop, we mark the stream as stopped and return the
            // remaining elements, irrespectively of boundaries.
            if let Some(stream_stop) = this.stream_stop.as_mut().as_pin_mut() {
                if stream_stop.poll(cx).is_ready() {
                    info!("the stream has been forcefully stopped");
                    *this.stream_stopped = true;

                    return if !this.items.is_empty() {
                        Poll::Ready(Some(std::mem::take(this.items)))
                    } else {
                        Poll::Ready(None)
                    };
                }
            }

            if *this.reset_timer {
                this.deadline
                    .set(Some(sleep(this.batch_config.max_batch_fill_time)));
                *this.reset_timer = false;
            }
            if this.items.is_empty() {
                this.items.reserve_exact(this.batch_config.max_batch_size);
            }
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => break,
                Poll::Ready(Some(item)) => {
                    let is_last_in_batch = item.is_on_boundary();
                    this.items.push(item);
                    if this.items.len() >= this.batch_config.max_batch_size && is_last_in_batch {
                        *this.reset_timer = true;
                        return Poll::Ready(Some(std::mem::take(this.items)));
                    }
                }
                Poll::Ready(None) => {
                    let last = if this.items.is_empty() {
                        None
                    } else {
                        *this.reset_timer = true;
                        Some(std::mem::take(this.items))
                    };

                    *this.inner_stream_ended = true;

                    return Poll::Ready(last);
                }
            }
        }

        if !this.items.is_empty() {
            if let Some(deadline) = this.deadline.as_pin_mut() {
                ready!(deadline.poll(cx));
            }

            let last_item = this.items.last().expect("missing last item");
            if last_item.is_on_boundary() {
                *this.reset_timer = true;
                return Poll::Ready(Some(std::mem::take(this.items)));
            }
        }

        Poll::Pending
    }
}
