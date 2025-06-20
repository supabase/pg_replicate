use config::shared::SourceConfig;
use sqlx::PgPool;
use thiserror::Error;

use crate::db::base::{serialize_to_db_as_json, DbSerializationError};
use crate::encryption::EncryptionKey;

use super::{
    sources::{create_source_txn, SourcesDbError},
    tenants::create_tenant_txn,
};

#[derive(Debug, Error)]
pub enum TenantSourceDbError {
    #[error("Error while dealing with PostgreSQL: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("Error while serializing source config: {0}")]
    DbSerializationError(#[from] DbSerializationError),

    #[error("Error while dealing with the source for this tenant: {0}")]
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
    let source_config = serialize_to_db_as_json(source_config, encryption_key)?;

    let mut txn = pool.begin().await?;
    let tenant_id = create_tenant_txn(&mut txn, tenant_id, tenant_name).await?;
    let source_id = create_source_txn(&mut txn, &tenant_id, source_name, source_config).await?;
    txn.commit().await?;

    Ok((tenant_id, source_id))
}
