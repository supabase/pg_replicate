use std::time::Duration;

pub mod data_pipeline;
pub mod stream;

/// A trait to indicate which items in a stream can be the last in a batch.
pub trait BatchBoundary: Sized {
    fn is_last_in_batch(&self) -> bool;
}

// For an item wrapped in a result we fall back to the item
// for the Ok variant and always return true for an Err variant
// to fail fast as this batch is anyway going to fail.
impl<T: BatchBoundary, E> BatchBoundary for Result<T, E> {
    fn is_last_in_batch(&self) -> bool {
        match self {
            Ok(v) => v.is_last_in_batch(),
            Err(_) => true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BatchConfig {
    max_batch_size: usize,
    max_batch_fill_time: Duration,
}

impl BatchConfig {
    pub fn new(max_batch_size: usize, max_batch_fill_time: Duration) -> BatchConfig {
        BatchConfig {
            max_batch_size,
            max_batch_fill_time,
        }
    }
}
