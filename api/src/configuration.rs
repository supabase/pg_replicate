use std::fmt::{self, Display};

use base64::{prelude::BASE64_STANDARD, Engine};
use secrecy::{ExposeSecret, Secret};
use serde::{
    de::{self, MapAccess, Unexpected, Visitor},
    Deserialize, Deserializer,
};
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use thiserror::Error;

#[derive(serde::Deserialize, Clone)]
pub struct EncryptionKey {
    pub id: u32,
    pub key: String,
}

impl Display for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "    id: {}", self.id)?;
        writeln!(f, "    key: REDACTED")
    }
}

const API_KEY_LENGTH_IN_BYTES: usize = 32;

pub struct ApiKey {
    pub key: [u8; API_KEY_LENGTH_IN_BYTES],
}

#[derive(Debug, Error)]
pub enum ApiKeyConversionError {
    #[error("api key is not base64 encoded")]
    NotBase64Encoded,

    #[error("expected length of api key is 32, but actual length is {0}")]
    LengthNot32IBytes(usize),
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
                    de::Error::invalid_value(Unexpected::Str(key_str), &"base64 encoded 32 bytes")
                })?;
                Ok(key)
            }
        }

        const FIELDS: &[&str] = &["key"];
        deserializer.deserialize_struct("ApiKey", FIELDS, ApiKeyVisitor)
    }
}

#[derive(serde::Deserialize, Clone)]
pub struct Settings {
    pub database: DatabaseSettings,
    pub application: ApplicationSettings,
    pub worker: WorkerSettings,
    pub encryption_key: EncryptionKey,
    pub api_key: String,
}

impl Display for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "configuration:")?;
        writeln!(f, "  database:\n{}", self.database)?;
        writeln!(f, "  application:\n{}", self.application)?;
        writeln!(f, "  worker:\n{}", self.worker)?;
        writeln!(f, "  encryption_key:\n{}", self.encryption_key)?;
        writeln!(f, "  api_key: REDACTED")
    }
}

#[derive(serde::Deserialize, Clone)]
pub struct DatabaseSettings {
    /// Host on which Postgres is running
    pub host: String,

    /// Port on which Postgres is running
    pub port: u16,

    /// Postgres database name
    pub name: String,

    /// Postgres database user name
    pub username: String,

    /// Postgres database user password
    pub password: Option<Secret<String>>,

    /// Whether to enable ssl or not
    pub require_ssl: bool,
}

impl Display for DatabaseSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "    host: {}", self.host)?;
        writeln!(f, "    port: {}", self.port)?;
        writeln!(f, "    name: {}", self.name)?;
        writeln!(f, "    username: {}", self.username)?;
        writeln!(f, "    password: REDACTED")?;
        writeln!(f, "    require_ssl: {}", self.require_ssl)
    }
}

impl DatabaseSettings {
    pub fn without_db(&self) -> PgConnectOptions {
        let ssl_mode = if self.require_ssl {
            PgSslMode::Require
        } else {
            PgSslMode::Prefer
        };
        let options = PgConnectOptions::new_without_pgpass()
            .host(&self.host)
            .username(&self.username)
            .port(self.port)
            .ssl_mode(ssl_mode);
        if let Some(password) = &self.password {
            options.password(password.expose_secret())
        } else {
            options
        }
    }

    pub fn with_db(&self) -> PgConnectOptions {
        self.without_db().database(&self.name)
    }
}

#[derive(serde::Deserialize, Clone)]
pub struct ApplicationSettings {
    /// host the api listens on
    pub host: String,

    /// port the api listens on
    pub port: u16,
}

impl Display for ApplicationSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "    host: {}", self.host)?;
        writeln!(f, "    port: {}", self.port)
    }
}

#[derive(serde::Deserialize, Clone)]
pub struct WorkerSettings {
    /// interval after which the worker looks in the queue for tasks
    pub poll_interval_secs: u64,
}

impl Display for WorkerSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "    poll_interval_secs: {}", self.poll_interval_secs)
    }
}

pub fn get_settings<'a, T: serde::Deserialize<'a>>() -> Result<T, config::ConfigError> {
    let base_path = std::env::current_dir().expect("Failed to determine the current directory");
    let configuration_directory = base_path.join("configuration");

    // Detect the running environment.
    // Default to `dev` if unspecified.
    let environment: Environment = std::env::var("APP_ENVIRONMENT")
        .unwrap_or_else(|_| DEV_ENV_NAME.into())
        .try_into()
        .expect("Failed to parse APP_ENVIRONMENT.");

    let environment_filename = format!("{}.yaml", environment.as_str());
    let settings = config::Config::builder()
        .add_source(config::File::from(
            configuration_directory.join("base.yaml"),
        ))
        .add_source(config::File::from(
            configuration_directory.join(environment_filename),
        ))
        // Add in settings from environment variables (with a prefix of APP and '__' as separator)
        // E.g. `APP_SINK__BIG_QUERY__PROJECT_ID=my-project-id would set `Settings { sink: BigQuery { project_id }}` to my-project-id
        .add_source(
            config::Environment::with_prefix("APP")
                .prefix_separator("_")
                .separator("__"),
        )
        .build()?;

    settings.try_deserialize::<T>()
}

const DEV_ENV_NAME: &str = "dev";
const PROD_ENV_NAME: &str = "prod";

/// The possible runtime environment for our application.
pub enum Environment {
    Dev,
    Prod,
}

impl Environment {
    pub fn as_str(&self) -> &'static str {
        match self {
            Environment::Dev => DEV_ENV_NAME,
            Environment::Prod => PROD_ENV_NAME,
        }
    }
}

impl TryFrom<String> for Environment {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "dev" => Ok(Self::Dev),
            "prod" => Ok(Self::Prod),
            other => Err(format!(
                "{other} is not a supported environment. Use either `{DEV_ENV_NAME}` or `{PROD_ENV_NAME}`.",
            )),
        }
    }
}
