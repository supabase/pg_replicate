use aws_lc_rs::{aead::Nonce, error::Unspecified};
use base64::{prelude::BASE64_STANDARD, DecodeError, Engine};
use sqlx::{
    postgres::{PgConnectOptions, PgSslMode},
    PgPool,
};
use std::{
    fmt::{Debug, Formatter},
    str::{from_utf8, Utf8Error},
};
use thiserror::Error;

use crate::encryption::{decrypt, encrypt, EncryptedValue, EncryptionKey};

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
enum SourceConfigInDb {
    Postgres {
        /// Host on which Postgres is running
        host: String,

        /// Port on which Postgres is running
        port: u16,

        /// Postgres database name
        name: String,

        /// Postgres database user name
        username: String,

        /// Postgres database user password
        password: Option<EncryptedValue>,

        /// Postgres slot name
        slot_name: String,
    },
}

impl SourceConfigInDb {
    fn into_config(self, encryption_key: &EncryptionKey) -> Result<SourceConfig, SourcesDbError> {
        let SourceConfigInDb::Postgres {
            host,
            port,
            name,
            username,
            password: encrypted_password,
            slot_name,
        } = self;

        let decrypted_password = encrypted_password
            .map(|encrypted_password| {
                if encrypted_password.id != encryption_key.id {
                    return Err(SourcesDbError::MismatchedKeyId(
                        encrypted_password.id,
                        encryption_key.id,
                    ));
                }
                let encrypted_password_bytes = BASE64_STANDARD.decode(encrypted_password.value)?;
                let nonce = Nonce::try_assume_unique_for_key(
                    &BASE64_STANDARD.decode(encrypted_password.nonce)?,
                )?;
                let decrypted_password = from_utf8(&decrypt(
                    encrypted_password_bytes,
                    nonce,
                    &encryption_key.key,
                )?)?
                .to_string();
                Ok(decrypted_password)
            })
            .transpose()?;

        Ok(SourceConfig::Postgres {
            host,
            port,
            name,
            username,
            password: decrypted_password,
            slot_name,
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SourceConfig {
    Postgres {
        /// Host on which Postgres is running
        host: String,

        /// Port on which Postgres is running
        port: u16,

        /// Postgres database name
        name: String,

        /// Postgres database user name
        username: String,

        /// Postgres database user password
        password: Option<String>,

        /// Postgres slot name
        slot_name: String,
    },
}

impl SourceConfig {
    pub fn connect_options(&self) -> PgConnectOptions {
        match self {
            SourceConfig::Postgres {
                host,
                port,
                name,
                username,
                password,
                slot_name: _,
            } => {
                let ssl_mode = PgSslMode::Prefer;

                let options = PgConnectOptions::new_without_pgpass()
                    .host(host)
                    .port(*port)
                    .database(name)
                    .username(username)
                    .ssl_mode(ssl_mode);
                if let Some(password) = password {
                    options.password(password)
                } else {
                    options
                }
            }
        }
    }

    fn into_db_config(
        self,
        encryption_key: &EncryptionKey,
    ) -> Result<SourceConfigInDb, Unspecified> {
        let SourceConfig::Postgres {
            host,
            port,
            name,
            username,
            password,
            slot_name,
        } = self;

        let encrypted_password = password
            .map(|password| {
                let (encrypted_password, nonce) =
                    encrypt(password.as_bytes(), &encryption_key.key)?;
                let encrypted_encoded_password = BASE64_STANDARD.encode(encrypted_password);
                let encoded_nonce = BASE64_STANDARD.encode(nonce.as_ref());
                Ok::<EncryptedValue, Unspecified>(EncryptedValue {
                    id: encryption_key.id,
                    nonce: encoded_nonce,
                    value: encrypted_encoded_password,
                })
            })
            .transpose()?;

        Ok(SourceConfigInDb::Postgres {
            host,
            port,
            name,
            username,
            password: encrypted_password,
            slot_name,
        })
    }
}

impl Debug for SourceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceConfig::Postgres {
                host,
                port,
                name,
                username,
                password: _,
                slot_name,
            } => f
                .debug_struct("Postgres")
                .field("host", host)
                .field("port", port)
                .field("name", name)
                .field("username", username)
                .field("password", &"REDACTED")
                .field("slot_name", slot_name)
                .finish(),
        }
    }
}

pub struct Source {
    pub id: i64,
    pub tenant_id: String,
    pub name: String,
    pub config: SourceConfig,
}

#[derive(Debug, Error)]
pub enum SourcesDbError {
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

pub async fn create_source(
    pool: &PgPool,
    tenant_id: &str,
    name: &str,
    config: SourceConfig,
    encryption_key: &EncryptionKey,
) -> Result<i64, SourcesDbError> {
    let db_config = config.into_db_config(encryption_key)?;
    let db_config = serde_json::to_value(db_config).expect("failed to serialize config");
    let record = sqlx::query!(
        r#"
        insert into app.sources (tenant_id, name, config)
        values ($1, $2, $3)
        returning id
        "#,
        tenant_id,
        name,
        db_config
    )
    .fetch_one(pool)
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
            let config: SourceConfigInDb = serde_json::from_value(r.config)?;
            let config = config.into_config(encryption_key)?;
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
    let db_config = config.into_db_config(encryption_key)?;
    let db_config = serde_json::to_value(db_config).expect("failed to serialize config");
    let record = sqlx::query!(
        r#"
        update app.sources
        set config = $1, name = $2
        where tenant_id = $3 and id = $4
        returning id
        "#,
        db_config,
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
        let config: SourceConfigInDb = serde_json::from_value(record.config)?;
        let config = config.into_config(encryption_key)?;
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
