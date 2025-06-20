use aws_lc_rs::error::Unspecified;
use sqlx::PgPool;
use thiserror::Error;

use crate::encryption::EncryptionKey;

use super::{
    sources::{create_source_txn, SourcesDbError},
    tenants::create_tenant_txn,
};

#[derive(Debug, Error)]
pub enum TenantSourceDbError {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("encryption error: {0}")]
    Encryption(#[from] Unspecified),

    #[error("sources error: {0}")]
    Sources(#[from] SourcesDbError),
}

pub async fn create_tenant_and_source(
    pool: &PgPool,
    tenant_id: &str,
    tenant_name: &str,
    source_name: &str,
    source_config: SourceConfig,
    encryption_key: &EncryptionKey,
) -> Result<(String, i64), TenantSourceDbError> {
    let db_config = source_config.into_db_config(encryption_key)?;
    let db_config = serde_json::to_value(db_config).expect("failed to serialize config");
    let mut txn = pool.begin().await?;
    let tenant_id = create_tenant_txn(&mut txn, tenant_id, tenant_name).await?;
    let source_id = create_source_txn(&mut txn, &tenant_id, source_name, db_config).await?;
    txn.commit().await?;
    Ok((tenant_id, source_id))
}
