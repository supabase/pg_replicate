use base64::{prelude::BASE64_STANDARD, Engine};
use postgres::sqlx::config::PgConnectionConfig;
use serde::de::{MapAccess, Visitor};
use serde::{de, Deserialize, Deserializer};
use std::fmt;
use thiserror::Error;

const API_KEY_LENGTH_IN_BYTES: usize = 32;

#[derive(Deserialize, Clone)]
pub struct ApiConfig {
    pub database: PgConnectionConfig,
    pub application: ApplicationSettings,
    pub encryption_key: EncryptionKey,
    pub api_key: String,
}

#[derive(Deserialize, Clone)]
pub struct ApplicationSettings {
    /// Host the api listens on.
    pub host: String,
    /// Port the api listens on.
    pub port: u16,
}

impl fmt::Display for ApplicationSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "    host: {}", self.host)?;
        writeln!(f, "    port: {}", self.port)
    }
}

#[derive(Deserialize, Clone)]
pub struct EncryptionKey {
    pub id: u32,
    pub key: String,
}

#[derive(Debug, Error)]
pub enum ApiKeyConversionError {
    #[error("api key is not base64 encoded")]
    NotBase64Encoded,

    #[error("expected length of api key is 32, but actual length is {0}")]
    LengthNot32IBytes(usize),
}

pub struct ApiKey {
    pub key: [u8; API_KEY_LENGTH_IN_BYTES],
}

impl TryFrom<&str> for ApiKey {
    type Error = ApiKeyConversionError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let key = BASE64_STANDARD
            .decode(value)
            .map_err(|_| ApiKeyConversionError::NotBase64Encoded)?;

        if key.len() != API_KEY_LENGTH_IN_BYTES {
            return Err(ApiKeyConversionError::LengthNot32IBytes(key.len()));
        }

        Ok(ApiKey {
            key: key
                .try_into()
                .expect("failed to convert api key into array"),
        })
    }
}

impl<'de> Deserialize<'de> for ApiKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Key,
        }

        struct ApiKeyVisitor;

        impl<'de> Visitor<'de> for ApiKeyVisitor {
            type Value = ApiKey;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct ApiKey")
            }

            fn visit_map<V>(self, mut map: V) -> Result<ApiKey, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut key: Option<&str> = None;
                while let Some(map_key) = map.next_key()? {
                    match map_key {
                        Field::Key => {
                            if key.is_some() {
                                return Err(de::Error::duplicate_field("key"));
                            }
                            key = Some(map.next_value()?);
                        }
                    }
                }
                let key_str = key.ok_or_else(|| de::Error::missing_field("key"))?;
                let key = key_str.try_into().map_err(|_| {
                    de::Error::invalid_value(
                        de::Unexpected::Str(key_str),
                        &"base64 encoded 32 bytes",
                    )
                })?;
                Ok(key)
            }
        }

        const FIELDS: &[&str] = &["key"];
        deserializer.deserialize_struct("ApiKey", FIELDS, ApiKeyVisitor)
    }
}
