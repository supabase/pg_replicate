use aws_lc_rs::{aead::Nonce, error::Unspecified};
use base64::{prelude::BASE64_STANDARD, DecodeError, Engine};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use std::{
    fmt::{Debug, Formatter},
    str::{from_utf8, Utf8Error},
};
use thiserror::Error;

use crate::encryption::{decrypt, encrypt, EncryptedValue, EncryptionKey};

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DestinationConfig {
    BigQuery {
        /// BigQuery project id
        project_id: String,

        /// BigQuery dataset id
        dataset_id: String,

        /// BigQuery service account key
        service_account_key: String,

        /// The max_staleness parameter for BigQuery: https://cloud.google.com/bigquery/docs/change-data-capture#create-max-staleness
        #[serde(skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>,
    },
}

impl DestinationConfig {
    pub fn into_db_config(
        self,
        encryption_key: &EncryptionKey,
    ) -> Result<DestinationConfigInDb, Unspecified> {
        let DestinationConfig::BigQuery {
            project_id,
            dataset_id,
            service_account_key,
            max_staleness_mins,
        } = self;

        let (encrypted_sa_key, nonce) =
            encrypt(service_account_key.as_bytes(), &encryption_key.key)?;
        let encrypted_encoded_sa_key = BASE64_STANDARD.encode(encrypted_sa_key);
        let encoded_nonce = BASE64_STANDARD.encode(nonce.as_ref());
        let encrypted_sa_key = EncryptedValue {
            id: encryption_key.id,
            nonce: encoded_nonce,
            value: encrypted_encoded_sa_key,
        };

        Ok(DestinationConfigInDb::BigQuery {
            project_id,
            dataset_id,
            service_account_key: encrypted_sa_key,
            max_staleness_mins,
        })
    }
}

impl Debug for DestinationConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key: _,
                max_staleness_mins,
            } => f
                .debug_struct("BigQuery")
                .field("project_id", project_id)
                .field("dataset_id", dataset_id)
                .field("service_account_key", &"REDACTED")
                .field("max_staleness_mins", max_staleness_mins)
                .finish(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DestinationConfigInDb {
    BigQuery {
        /// BigQuery project id
        project_id: String,

        /// BigQuery dataset id
        dataset_id: String,

        /// BigQuery service account key
        service_account_key: EncryptedValue,

        /// The max_staleness parameter for BigQuery: https://cloud.google.com/bigquery/docs/change-data-capture#create-max-staleness
        #[serde(skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>,
    },
}

impl DestinationConfigInDb {
    fn into_config(
        self,
        encryption_key: &EncryptionKey,
    ) -> Result<DestinationConfig, DestinationsDbError> {
        let DestinationConfigInDb::BigQuery {
            project_id,
            dataset_id,
            service_account_key: encrypted_sa_key,
            max_staleness_mins,
        } = self;

        if encrypted_sa_key.id != encryption_key.id {
            return Err(DestinationsDbError::MismatchedKeyId(
                encrypted_sa_key.id,
                encryption_key.id,
            ));
        }

        let encrypted_sa_key_bytes = BASE64_STANDARD.decode(encrypted_sa_key.value)?;
        let nonce =
            Nonce::try_assume_unique_for_key(&BASE64_STANDARD.decode(encrypted_sa_key.nonce)?)?;
        let decrypted_sa_key = from_utf8(&decrypt(
            encrypted_sa_key_bytes,
            nonce,
            &encryption_key.key,
        )?)?
        .to_string();

        Ok(DestinationConfig::BigQuery {
            project_id,
            dataset_id,
            service_account_key: decrypted_sa_key,
            max_staleness_mins,
        })
    }
}

#[derive(Debug, Error)]
pub enum DestinationsDbError {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("encryption error: {0}")]
    Encryption(#[from] Unspecified),

    #[error("invalid source config in db")]
    InvalidConfig(#[from] serde_json::Error),

    #[error("mismatched key id. Expected: {0}, actual: {1}")]
    MismatchedKeyId(u32, u32),

    #[error("base64 decode error: {0}")]
    Base64Decode(#[from] DecodeError),

    #[error("utf8 error: {0}")]
    Utf8(#[from] Utf8Error),
}

pub struct Destination {
    pub id: i64,
    pub tenant_id: String,
    pub name: String,
    pub config: DestinationConfig,
}

pub async fn create_destination(
    pool: &PgPool,
    tenant_id: &str,
    name: &str,
    config: DestinationConfig,
    encryption_key: &EncryptionKey,
) -> Result<i64, DestinationsDbError> {
    let db_config = config.into_db_config(encryption_key)?;
    let db_config = serde_json::to_value(db_config).expect("failed to serialize config");
    let mut txn = pool.begin().await?;
    let res = create_destination_txn(&mut txn, tenant_id, name, db_config).await;
    txn.commit().await?;
    res
}

pub async fn create_destination_txn(
    txn: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    name: &str,
    destination_config: Value,
) -> Result<i64, DestinationsDbError> {
    let record = sqlx::query!(
        r#"
        insert into app.destinations (tenant_id, name, config)
        values ($1, $2, $3)
        returning id
        "#,
        tenant_id,
        name,
        destination_config
    )
    .fetch_one(&mut **txn)
    .await?;

    Ok(record.id)
}

pub async fn read_destination(
    pool: &PgPool,
    tenant_id: &str,
    destination_id: i64,
    encryption_key: &EncryptionKey,
) -> Result<Option<Destination>, DestinationsDbError> {
    let record = sqlx::query!(
        r#"
        select id, tenant_id, name, config
        from app.destinations
        where tenant_id = $1 and id = $2
        "#,
        tenant_id,
        destination_id,
    )
    .fetch_optional(pool)
    .await?;

    let destination = record
        .map(|r| {
            let config: DestinationConfigInDb = serde_json::from_value(r.config)?;
            let config = config.into_config(encryption_key)?;
            let source = Destination {
                id: r.id,
                tenant_id: r.tenant_id,
                name: r.name,
                config,
            };
            Ok::<Destination, DestinationsDbError>(source)
        })
        .transpose()?;
    Ok(destination)
}

pub async fn update_destination(
    pool: &PgPool,
    tenant_id: &str,
    name: &str,
    destination_id: i64,
    destination_config: DestinationConfig,
    encryption_key: &EncryptionKey,
) -> Result<Option<i64>, DestinationsDbError> {
    let destination_config = destination_config.into_db_config(encryption_key)?;
    let destination_config =
        serde_json::to_value(destination_config).expect("failed to serialize config");
    let mut txn = pool.begin().await?;
    let res = update_destination_txn(
        &mut txn,
        tenant_id,
        name,
        destination_id,
        destination_config,
    )
    .await;
    txn.commit().await?;
    res
}

pub async fn update_destination_txn(
    txn: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    name: &str,
    destination_id: i64,
    destination_config: Value,
) -> Result<Option<i64>, DestinationsDbError> {
    let record = sqlx::query!(
        r#"
        update app.destinations
        set config = $1, name = $2
        where tenant_id = $3 and id = $4
        returning id
        "#,
        destination_config,
        name,
        tenant_id,
        destination_id
    )
    .fetch_optional(&mut **txn)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_destination(
    pool: &PgPool,
    tenant_id: &str,
    destination_id: i64,
) -> Result<Option<i64>, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        delete from app.destinations
        where tenant_id = $1 and id = $2
        returning id
        "#,
        tenant_id,
        destination_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn read_all_destinations(
    pool: &PgPool,
    tenant_id: &str,
    encryption_key: &EncryptionKey,
) -> Result<Vec<Destination>, DestinationsDbError> {
    let records = sqlx::query!(
        r#"
        select id, tenant_id, name, config
        from app.destinations
        where tenant_id = $1
        "#,
        tenant_id,
    )
    .fetch_all(pool)
    .await?;

    let mut destinations = Vec::with_capacity(records.len());
    for record in records {
        let config: DestinationConfigInDb = serde_json::from_value(record.config)?;
        let config = config.into_config(encryption_key)?;
        let source = Destination {
            id: record.id,
            tenant_id: record.tenant_id,
            name: record.name,
            config,
        };
        destinations.push(source);
    }

    Ok(destinations)
}

pub async fn destination_exists(
    pool: &PgPool,
    tenant_id: &str,
    destination_id: i64,
) -> Result<bool, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        select exists (select id
        from app.destinations
        where tenant_id = $1 and id = $2) as "exists!"
        "#,
        tenant_id,
        destination_id,
    )
    .fetch_one(pool)
    .await?;

    Ok(record.exists)
}

#[cfg(test)]
mod tests {
    use crate::db::destinations::DestinationConfig;

    #[test]
    pub fn deserialize_settings_test() {
        let settings = r#"{
            "big_query": {
                "project_id": "project-id",
                "dataset_id": "dataset-id",
                "service_account_key": "service-account-key"
            }
        }"#;
        let actual = serde_json::from_str::<DestinationConfig>(settings);
        let expected = DestinationConfig::BigQuery {
            project_id: "project-id".to_string(),
            dataset_id: "dataset-id".to_string(),
            service_account_key: "service-account-key".to_string(),
            max_staleness_mins: None,
        };
        assert!(actual.is_ok());
        assert_eq!(expected, actual.unwrap());
    }

    #[test]
    pub fn serialize_settings_test() {
        let actual = DestinationConfig::BigQuery {
            project_id: "project-id".to_string(),
            dataset_id: "dataset-id".to_string(),
            service_account_key: "service-account-key".to_string(),
            max_staleness_mins: None,
        };
        let expected = r#"{"big_query":{"project_id":"project-id","dataset_id":"dataset-id","service_account_key":"service-account-key"}}"#;
        let actual = serde_json::to_string(&actual);
        assert!(actual.is_ok());
        assert_eq!(expected, actual.unwrap());
    }
}
