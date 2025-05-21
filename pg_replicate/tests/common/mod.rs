use std::time::{Duration, Instant};
use tokio::time::sleep;

pub mod database;
pub mod pipeline;
pub mod sink;
pub mod table;

/// The maximum time in seconds for which we should wait for a condition to be met
/// in tests.
const MAX_ASSERTION_DURATION: Duration = Duration::from_secs(20);

/// The frequency at which we should check for a condition to be met in tests.
const ASSERTION_FREQUENCY_DURATION: Duration = Duration::from_millis(10);

/// Wait for a condition to be met within the maximum timeout.
pub async fn wait_for_condition<F>(condition: F)
where
    F: Fn() -> bool,
{
    let start = Instant::now();
    while start.elapsed() < MAX_ASSERTION_DURATION {
        if condition() {
            return;
        }

        sleep(ASSERTION_FREQUENCY_DURATION).await;
    }

    assert!(false, "Failed to process all events within timeout")
}
