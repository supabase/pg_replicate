pub use executor::{DuckDbExecutorError, DuckDbRequest};
pub use sink::DuckDbSink;

use super::{Sink, SinkError};

mod executor;
mod sink;
