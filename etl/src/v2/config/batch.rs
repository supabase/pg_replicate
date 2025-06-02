use std::time::Duration;

#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub max_batch_size: usize,
    pub max_batch_fill_time: Duration,
}

impl BatchConfig {
    pub fn new(max_batch_size: usize, max_batch_fill_time: Duration) -> BatchConfig {
        BatchConfig {
            max_batch_size,
            max_batch_fill_time,
        }
    }
}
