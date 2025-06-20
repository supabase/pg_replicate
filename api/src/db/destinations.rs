use sqlx::{PgPool, Postgres, Transaction};
use std::{
    fmt::{Debug, Formatter},
    str::{from_utf8, Utf8Error},
};
use thiserror::Error;
use config::shared::DestinationConfig;
use crate::db::base::{decrypt_and_deserialize_from_value, encrypt_and_serialize, DbDeserializationError, DbSerializationError, Decryptable, Encryptable, ToDbError, ToMemoryError};
use crate::db::sources::EncryptedSourceConfig;
use crate::encryption::{decrypt, encrypt, encrypt_text, EncryptedValue, EncryptionKey};

impl Encryptable<EncryptedDestinationConfig> for DestinationConfig {
    
    fn encrypt(self, encryption_key: &EncryptionKey) -> Result<EncryptedDestinationConfig, ToDbError> {
        match self {
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins
            } => {
                let mut encrypted_service_account_key = encrypt_text(service_account_key, encryption_key)?;
                
                Ok(EncryptedDestinationConfig::BigQuery {
                    project_id,
                    dataset_id,
                    service_account_key: encrypted_service_account_key,
                    max_staleness_mins,
                })
            }
            _ => {}
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EncryptedDestinationConfig {
    BigQuery {
        project_id: String,
        dataset_id: String,
        service_account_key: EncryptedValue,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>,
    },
}

impl Decryptable<DestinationConfig> for EncryptedDestinationConfig {
    
    fn decrypt(self, encryption_key: &EncryptionKey) -> Result<DestinationConfig, ToMemoryError> {
        todo!()
    }
}

#[derive(Debug, Error)]
pub enum DestinationsDbError {
    #[error("Error while interacting with PostgreSQL for destinations: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("Error while serializing destination config: {0}")]
    DbSerializationError(#[from] DbSerializationError),

    #[error("Error while deserializing destination config: {0}")]
    DbDeserializationError(#[from] DbDeserializationError),
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
    let config = encrypt_and_serialize(config, encryption_key)?;
    
    let mut txn = pool.begin().await?;
    let res = create_destination_txn(&mut txn, tenant_id, name, config).await;
    txn.commit().await?;
    
    res
}

pub async fn create_destination_txn(
    txn: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    name: &str,
    config: serde_json::Value,
) -> Result<i64, DestinationsDbError> {
    let record = sqlx::query!(
        r#"
        insert into app.destinations (tenant_id, name, config)
        values ($1, $2, $3)
        returning id
        "#,
        tenant_id,
        name,
        config
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

    let destination = match record {
        Some(record) => { 
            let config = decrypt_and_deserialize_from_value::<EncryptedDestinationConfig, DestinationConfig>(record.config, encryption_key)?;
            
            let destination = Destination {
                id: record.id,
                tenant_id: record.tenant_id,
                name: record.name,
                config,
            };
            
            Some(destination)
        },
        None => None
    };
    
    Ok(destination)
}

pub async fn update_destination(
    pool: &PgPool,
    tenant_id: &str,
    name: &str,
    destination_id: i64,
    config: DestinationConfig,
    encryption_key: &EncryptionKey,
) -> Result<Option<i64>, DestinationsDbError> {
    let config = encrypt_and_serialize(config, encryption_key)?;
   
    let mut txn = pool.begin().await?;
    let res = update_destination_txn(
        &mut txn,
        tenant_id,
        name,
        destination_id,
        config,
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
    config: serde_json::Value,
) -> Result<Option<i64>, DestinationsDbError> {
    let record = sqlx::query!(
        r#"
        update app.destinations
        set config = $1, name = $2
        where tenant_id = $3 and id = $4
        returning id
        "#,
        config,
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

// TODO: rewrite tests.
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
