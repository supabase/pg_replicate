use crate::v2::config::batch::BatchConfig;

#[derive(Debug)]
pub struct PipelineConfig {
    pub batch_config: BatchConfig,
}
