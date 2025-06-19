use std::time::Duration;

#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub max_size: usize,
    pub max_fill: Duration,
}

impl BatchConfig {
    pub fn new(max_size: usize, max_fill: Duration) -> BatchConfig {
        BatchConfig { max_size, max_fill }
    }
}

impl Default for BatchConfig {
    fn default() -> BatchConfig {
        Self {
            max_size: 1000,
            max_fill: Duration::from_secs(1),
        }
    }
}
