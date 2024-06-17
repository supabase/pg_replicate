use futures::{ready, Future, Stream};
use tokio::time::{sleep, Sleep};

use core::pin::Pin;
use core::task::{Context, Poll};
use futures::stream::{Fuse, StreamExt};
use pin_project_lite::pin_project;
use std::time::Duration;

/// A trait to indicate which items in a stream can be the last in a batch.
pub trait BatchBoundary: Sized {
    fn is_last_in_batch(&self) -> bool;
}

// Implementation adapted from https://github.com/tokio-rs/tokio/blob/master/tokio-stream/src/stream_ext/chunks_timeout.rs
pin_project! {
    /// Adapter stream which batches the items of the underlying stream when it
    /// reaches max_size or when a timeout expires. The underlying streams items
    /// must implement [`BatchBoundary`]. A batch is guaranteed to end on an
    /// item which returns true from [`BatchBoundary::is_last_in_batch`]
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
    pub struct BatchTimeout<B: BatchBoundary, S: Stream<Item = B>> {
        #[pin]
        stream: Fuse<S>,
        #[pin]
        deadline: Option<Sleep>,
        timeout: Duration,
        items: Vec<S::Item>,
        max_batch_size: usize, // https://github.com/rust-lang/futures-rs/issues/1475
        reset_timer: bool,
    }
}

impl<B: BatchBoundary, S: Stream<Item = B>> BatchTimeout<B, S> {
    pub fn new(stream: S, max_batch_size: usize, timeout: Duration) -> Self {
        BatchTimeout {
            stream: stream.fuse(),
            deadline: None,
            timeout,
            items: Vec::with_capacity(max_batch_size),
            max_batch_size,
            reset_timer: true,
        }
    }
}

impl<B: BatchBoundary, S: Stream<Item = B>> Stream for BatchTimeout<B, S> {
    type Item = Vec<S::Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();
        loop {
            if *this.reset_timer {
                this.deadline.set(Some(sleep(*this.timeout)));
                *this.reset_timer = false;
            }
            if this.items.is_empty() {
                this.items.reserve_exact(*this.max_batch_size);
            }
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => break,
                Poll::Ready(Some(item)) => {
                    let is_last_in_batch = item.is_last_in_batch();
                    this.items.push(item);
                    if this.items.len() >= *this.max_batch_size && is_last_in_batch {
                        *this.reset_timer = true;
                        return Poll::Ready(Some(std::mem::take(this.items)));
                    }
                }
                Poll::Ready(None) => {
                    // Returning Some here is only correct because we fuse the inner stream.
                    let last = if this.items.is_empty() {
                        None
                    } else {
                        *this.reset_timer = true;
                        Some(std::mem::take(this.items))
                    };

                    return Poll::Ready(last);
                }
            }
        }

        if !this.items.is_empty() {
            if let Some(deadline) = this.deadline.as_pin_mut() {
                ready!(deadline.poll(cx));
            }

            let last_item = this.items.last().expect("missing last item");
            if last_item.is_last_in_batch() {
                *this.reset_timer = true;
                return Poll::Ready(Some(std::mem::take(this.items)));
            }
        }

        Poll::Pending
    }
}
