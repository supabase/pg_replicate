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

impl Default for BatchConfig {
    fn default() -> BatchConfig {
        Self {
            max_batch_size: 1000,
            max_batch_fill_time: Duration::from_secs(1),
        }
    }
}
