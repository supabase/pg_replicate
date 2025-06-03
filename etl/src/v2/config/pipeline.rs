use crate::v2::config::batch::BatchConfig;

#[derive(Debug, Default)]
pub struct PipelineConfig {
    pub batch_config: BatchConfig,
}
