use aws_lc_rs::error::Unspecified;
use sqlx::PgPool;
use thiserror::Error;

use crate::encryption::EncryptionKey;

use super::{
    pipelines::{create_pipeline_txn, PipelineConfig},
    sinks::{create_sink_txn, SinkConfig, SinksDbError},
};

#[derive(Debug, Error)]
pub enum SinkPipelineDbError {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("encryption error: {0}")]
    Encryption(#[from] Unspecified),

    #[error("sources error: {0}")]
    Sinks(#[from] SinksDbError),
}

#[expect(clippy::too_many_arguments)]
pub async fn create_sink_and_pipeline(
    pool: &PgPool,
    tenant_id: &str,
    source_id: i64,
    sink_name: &str,
    sink_config: SinkConfig,
    image_id: i64,
    publication_name: &str,
    pipeline_config: PipelineConfig,
    encryption_key: &EncryptionKey,
) -> Result<(i64, i64), SinkPipelineDbError> {
    let sink_config = sink_config.into_db_config(encryption_key)?;
    let sink_config = serde_json::to_value(sink_config).expect("failed to serialize config");
    let pipeline_config =
        serde_json::to_value(pipeline_config).expect("failed to serialize config");
    let mut txn = pool.begin().await?;
    let sink_id = create_sink_txn(&mut txn, tenant_id, sink_name, sink_config).await?;
    let pipeline_id = create_pipeline_txn(
        &mut txn,
        tenant_id,
        source_id,
        sink_id,
        image_id,
        publication_name,
        pipeline_config,
    )
    .await?;
    txn.commit().await?;
    Ok((sink_id, pipeline_id))
}
