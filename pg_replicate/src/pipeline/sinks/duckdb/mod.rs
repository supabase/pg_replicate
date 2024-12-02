pub use executor::{DuckDbExecutorError, DuckDbRequest};
pub use sink::DuckDbSink;

use super::Sink;

mod executor;
mod sink;
