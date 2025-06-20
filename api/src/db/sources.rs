use postgres::sqlx::config::PgConnectionConfig;
use secrecy::Secret;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use std::fmt::Debug;
use thiserror::Error;

use crate::db::base::{
    deserialize_from_db_json_value, serialize_to_db_as_json, DbDeserializationError,
    DbSerializationError, ToDb, ToDbError, ToMemory, ToMemoryError,
};
use crate::encryption::{decrypt_text, encrypt_text, EncryptedValue, EncryptionKey};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct SourceConfig {
    host: String,
    port: u16,
    name: String,
    username: String,
    password: Option<String>,
}

impl SourceConfig {
    pub fn into_connection_config(self) -> PgConnectionConfig {
        PgConnectionConfig {
            host: self.host,
            port: self.port,
            name: self.name,
            username: self.username,
            password: self.password.map(Secret::new),
            // By default, we enable ssl.
            require_ssl: true,
        }
    }
}

impl ToDb<SourceConfigInDb> for SourceConfig {
    fn to_db_type(self, encryption_key: &EncryptionKey) -> Result<SourceConfigInDb, ToDbError> {
        let mut encrypted_password = None;
        if let Some(password) = self.password {
            encrypted_password = Some(encrypt_text(password, encryption_key)?);
        }

        Ok(SourceConfigInDb {
            host: self.host,
            port: self.port,
            name: self.name,
            username: self.username,
            password: encrypted_password,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct SourceConfigInDb {
    host: String,
    port: u16,
    name: String,
    username: String,
    password: Option<EncryptedValue>,
}

impl ToMemory<SourceConfig> for SourceConfigInDb {
    fn to_memory_type(self, encryption_key: &EncryptionKey) -> Result<SourceConfig, ToMemoryError> {
        let mut decrypted_password = None;
        if let Some(password) = self.password {
            decrypted_password = Some(decrypt_text(password, encryption_key)?);
        }

        Ok(SourceConfig {
            host: self.host,
            port: self.port,
            name: self.name,
            username: self.username,
            password: decrypted_password,
        })
    }
}

#[derive(Debug)]
pub struct Source {
    pub id: i64,
    pub tenant_id: String,
    pub name: String,
    pub config: SourceConfig,
}

#[derive(Debug, Error)]
pub enum SourcesDbError {
    #[error("Error while dealing with PostgreSQL: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("Error while serializing source config: {0}")]
    DbSerializationError(#[from] DbSerializationError),

    #[error("Error while deserializing source config: {0}")]
    DbDeserializationError(#[from] DbDeserializationError),
}

pub async fn create_source(
    pool: &PgPool,
    tenant_id: &str,
    name: &str,
    config: SourceConfig,
    encryption_key: &EncryptionKey,
) -> Result<i64, SourcesDbError> {
    let config = serialize_to_db_as_json::<SourceConfig, SourceConfigInDb>(config, encryption_key)?;

    let mut txn = pool.begin().await?;
    let res = create_source_txn(&mut txn, tenant_id, name, config).await;
    txn.commit().await?;

    res
}

pub async fn create_source_txn(
    txn: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    name: &str,
    config: Value,
) -> Result<i64, SourcesDbError> {
    let record = sqlx::query!(
        r#"
        insert into app.sources (tenant_id, name, config)
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

pub async fn read_source(
    pool: &PgPool,
    tenant_id: &str,
    source_id: i64,
    encryption_key: &EncryptionKey,
) -> Result<Option<Source>, SourcesDbError> {
    let record = sqlx::query!(
        r#"
        select id, tenant_id, name, config
        from app.sources
        where tenant_id = $1 and id = $2
        "#,
        tenant_id,
        source_id,
    )
    .fetch_optional(pool)
    .await?;

    let source = record
        .map(|r| {
            let config = deserialize_from_db_json_value::<SourceConfigInDb, SourceConfig>(
                r.config,
                encryption_key,
            )?;
            let source = Source {
                id: r.id,
                tenant_id: r.tenant_id,
                name: r.name,
                config,
            };
            Ok::<Source, SourcesDbError>(source)
        })
        .transpose()?;
    Ok(source)
}

pub async fn update_source(
    pool: &PgPool,
    tenant_id: &str,
    name: &str,
    source_id: i64,
    config: SourceConfig,
    encryption_key: &EncryptionKey,
) -> Result<Option<i64>, SourcesDbError> {
    let config = serialize_to_db_as_json::<SourceConfig, SourceConfigInDb>(config, encryption_key)?;

    let record = sqlx::query!(
        r#"
        update app.sources
        set config = $1, name = $2
        where tenant_id = $3 and id = $4
        returning id
        "#,
        config,
        name,
        tenant_id,
        source_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_source(
    pool: &PgPool,
    tenant_id: &str,
    source_id: i64,
) -> Result<Option<i64>, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        delete from app.sources
        where tenant_id = $1 and id = $2
        returning id
        "#,
        tenant_id,
        source_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn read_all_sources(
    pool: &PgPool,
    tenant_id: &str,
    encryption_key: &EncryptionKey,
) -> Result<Vec<Source>, SourcesDbError> {
    let records = sqlx::query!(
        r#"
        select id, tenant_id, name, config
        from app.sources
        where tenant_id = $1
        "#,
        tenant_id,
    )
    .fetch_all(pool)
    .await?;

    let mut sources = Vec::with_capacity(records.len());
    for record in records {
        let config = deserialize_from_db_json_value::<SourceConfigInDb, SourceConfig>(
            record.config.clone(),
            encryption_key,
        )?;
        let source = Source {
            id: record.id,
            tenant_id: record.tenant_id,
            name: record.name,
            config,
        };
        sources.push(source);
    }

    Ok(sources)
}

pub async fn source_exists(
    pool: &PgPool,
    tenant_id: &str,
    source_id: i64,
) -> Result<bool, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        select exists (select id
        from app.sources
        where tenant_id = $1 and id = $2) as "exists!"
        "#,
        tenant_id,
        source_id
    )
    .fetch_one(pool)
    .await?;

    Ok(record.exists)
}

#[cfg(test)]
mod tests {
    use crate::db::sources::{SourceConfig, SourceConfigInDb};
    use crate::encryption::EncryptionKey;
    use aws_lc_rs::aead::RandomizedNonceKey;
    use serde_json;

    #[test]
    pub fn source_config_json_roundtrip() {
        let json = r#"{
            "host": "localhost",
            "port": 5432,
            "name": "postgres",
            "username": "postgres",
            "password": "postgres",
        }"#;
        let expected = SourceConfig {
            host: "localhost".to_string(),
            port: 5432,
            name: "postgres".to_string(),
            username: "postgres".to_string(),
            password: Some("postgres".to_string()),
        };

        let deserialized = serde_json::from_str::<SourceConfig>(json);
        assert!(deserialized.is_ok());
        assert_eq!(expected, deserialized.as_ref().unwrap().to_owned());

        let serialized = serde_json::to_string_pretty(&expected);
        assert!(serialized.is_ok());
        assert_eq!(json, serialized.unwrap());
    }

    #[test]
    pub fn source_config_in_db_password_encryption() {
        use crate::db::base::{deserialize_from_db_json_value, serialize_to_db_as_json};
        let key_bytes = [42u8; 32];
        let key = RandomizedNonceKey::new(&aws_lc_rs::aead::AES_256_GCM, &key_bytes).unwrap();
        let encryption_key = EncryptionKey { id: 1, key };

        let config = SourceConfig {
            host: "localhost".to_string(),
            port: 5432,
            name: "postgres".to_string(),
            username: "postgres".to_string(),
            password: Some("supersecret".to_string()),
        };

        // Serialize to db (should encrypt password)
        let config_in_db = serialize_to_db_as_json::<SourceConfig, SourceConfigInDb>(
            config.clone(),
            &encryption_key,
        )
        .unwrap();
        // Deserialize from db (should decrypt password)
        let deserialized_config = deserialize_from_db_json_value::<SourceConfigInDb, SourceConfig>(
            config_in_db,
            &encryption_key,
        )
        .unwrap();
        assert_eq!(config, deserialized_config);

        // None password case
        let config = SourceConfig {
            password: None,
            ..config.clone()
        };
        let config_in_db = serialize_to_db_as_json::<SourceConfig, SourceConfigInDb>(
            config.clone(),
            &encryption_key,
        )
        .unwrap();
        let deserialized_config = deserialize_from_db_json_value::<SourceConfigInDb, SourceConfig>(
            config_in_db,
            &encryption_key,
        )
        .unwrap();
        assert_eq!(config, deserialized_config);
    }
}
