use tracing::info;

use super::{Sink, SinkError};

pub struct StdoutSink;

// #[async_trait]
impl<TRO: Send + Sync> Sink<TRO> for StdoutSink {
    fn write_table_row(&self, _row: TRO) -> Result<(), SinkError> {
        // info!("{row:?}");
        info!("got row");
        Ok(())
    }
}
