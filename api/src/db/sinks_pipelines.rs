use aws_lc_rs::error::Unspecified;
use sqlx::PgPool;
use thiserror::Error;

use crate::encryption::EncryptionKey;

use super::{
    pipelines::{create_pipeline_txn, update_pipeline_txn, PipelineConfig},
    sinks::{
        create_destination_txn, update_destination_txn, DestinationConfig, DestinationsDbError,
    },
};

#[derive(Debug, Error)]
pub enum DestinationPipelineDbError {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("encryption error: {0}")]
    Encryption(#[from] Unspecified),

    #[error("sources error: {0}")]
    Destinations(#[from] DestinationsDbError),

    #[error("destination with id {0} not found")]
    DestinationNotFound(i64),

    #[error("pipeline with id {0} not found")]
    PipelineNotFound(i64),
}

#[expect(clippy::too_many_arguments)]
pub async fn create_destination_and_pipeline(
    pool: &PgPool,
    tenant_id: &str,
    source_id: i64,
    destination_name: &str,
    destination_config: DestinationConfig,
    image_id: i64,
    publication_name: &str,
    pipeline_config: PipelineConfig,
    encryption_key: &EncryptionKey,
) -> Result<(i64, i64), DestinationPipelineDbError> {
    let destination_config = destination_config.into_db_config(encryption_key)?;
    let destination_config =
        serde_json::to_value(destination_config).expect("failed to serialize config");
    let pipeline_config =
        serde_json::to_value(pipeline_config).expect("failed to serialize config");
    let mut txn = pool.begin().await?;
    let destination_id =
        create_destination_txn(&mut txn, tenant_id, destination_name, destination_config).await?;
    let pipeline_id = create_pipeline_txn(
        &mut txn,
        tenant_id,
        source_id,
        destination_id,
        image_id,
        publication_name,
        pipeline_config,
    )
    .await?;
    txn.commit().await?;
    Ok((destination_id, pipeline_id))
}

#[expect(clippy::too_many_arguments)]
pub async fn update_destination_and_pipeline(
    pool: &PgPool,
    tenant_id: &str,
    destination_id: i64,
    pipeline_id: i64,
    source_id: i64,
    destination_name: &str,
    destination_config: DestinationConfig,
    publication_name: &str,
    pipeline_config: PipelineConfig,
    encryption_key: &EncryptionKey,
) -> Result<(), DestinationPipelineDbError> {
    let destination_config = destination_config.into_db_config(encryption_key)?;
    let destination_config =
        serde_json::to_value(destination_config).expect("failed to serialize config");
    let pipeline_config =
        serde_json::to_value(pipeline_config).expect("failed to serialize config");
    let mut txn = pool.begin().await?;
    let destination_id_res = update_destination_txn(
        &mut txn,
        tenant_id,
        destination_name,
        destination_id,
        destination_config,
    )
    .await?;
    if destination_id_res.is_none() {
        txn.rollback().await?;
        return Err(DestinationPipelineDbError::DestinationNotFound(
            destination_id,
        ));
    };
    let pipeline_id_res = update_pipeline_txn(
        &mut txn,
        tenant_id,
        pipeline_id,
        source_id,
        destination_id,
        publication_name,
        pipeline_config,
    )
    .await?;

    if pipeline_id_res.is_none() {
        txn.rollback().await?;
        return Err(DestinationPipelineDbError::PipelineNotFound(pipeline_id));
    };

    txn.commit().await?;

    Ok(())
}
