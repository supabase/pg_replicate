use config::ConfigError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::environment::{Environment, DEV_ENV_NAME};

// Configuration constants
/// Directory containing configuration files.
const CONFIGURATION_DIR: &str = "configuration";
/// Name of the base configuration file.
const BASE_CONFIG_FILE: &str = "base.yaml";
/// Prefix for environment variable configuration overrides.
const ENV_PREFIX: &str = "APP";
/// Separator for environment variable prefix.
const ENV_PREFIX_SEPARATOR: &str = "_";
/// Separator for nested environment variable keys.
const ENV_SEPARATOR: &str = "__";

/// Errors that can occur when loading or validating a [`ReplicatorConfig`].
#[derive(Debug, Error)]
pub enum ReplicatorConfigError {
    /// Error loading configuration from files or environment variables.
    #[error("An error occurred while loading the replicator configuration: {0}")]
    Config(#[from] ConfigError),
    /// Error validating the loaded configuration.
    #[error("An error occurred while validating the replicator configuration: {0}")]
    Validation(#[from] ValidationError),
}

/// Errors that can occur during configuration validation.
#[derive(Debug, Error)]
pub enum ValidationError {
    /// TLS is enabled but no trusted root certificates are provided.
    #[error("Invalid TLS config: `trusted_root_certs` must be set when `enabled` is true")]
    MissingTrustedRootCerts,
}

/// Top-level configuration for the replicator service.
///
/// This struct aggregates all configuration sections, including source, destination, batch, and TLS settings.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ReplicatorConfig {
    /// Source Postgres connection settings.
    source: SourcePgConnectionConfig,
    /// Destination configuration.
    destination: DestinationConfig,
    /// Batch processing configuration.
    batch: BatchConfig,
    /// TLS configuration.
    tls: TlsConfig,
}

impl std::fmt::Debug for ReplicatorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicatorConfig")
            .field("source", &self.source)
            .field("destination", &self.destination)
            .field("batch", &self.batch)
            .field("tls", &self.tls)
            .finish()
    }
}

impl ReplicatorConfig {
    /// Loads the [`ReplicatorConfig`] from configuration files and environment variables.
    ///
    /// The configuration is loaded from the base and environment-specific YAML files in the configuration directory,
    /// then overridden by environment variables with the `APP` prefix. After loading, the configuration is validated.
    ///
    /// # Errors
    /// Returns [`ReplicatorConfigError`] if loading or validation fails.
    ///
    /// # Panics
    /// Panics if the current working directory cannot be determined or if the `APP_ENVIRONMENT` variable is invalid.
    pub fn load() -> Result<Self, ReplicatorConfigError> {
        let base_path = std::env::current_dir().expect("Failed to determine the current directory");
        let config_dir = base_path.join(CONFIGURATION_DIR);

        // Detect the running environment.
        // Default to `dev` if unspecified.
        let environment: Environment = std::env::var("APP_ENVIRONMENT")
            .unwrap_or_else(|_| DEV_ENV_NAME.into())
            .try_into()
            .expect("Failed to parse 'APP_ENVIRONMENT'.");

        let environment_filename = format!("{}.yaml", environment.as_str());
        let config = config::Config::builder()
            .add_source(config::File::from(
                config_dir.join(BASE_CONFIG_FILE),
            ))
            .add_source(config::File::from(
                config_dir.join(environment_filename),
            ))
            .add_source(
                config::Environment::with_prefix(ENV_PREFIX)
                    .prefix_separator(ENV_PREFIX_SEPARATOR)
                    .separator(ENV_SEPARATOR),
            )
            .build()?;

        let config: Self = config.try_deserialize()?;
        config.validate()?;
        Ok(config)
    }

    /// Validates the loaded [`ReplicatorConfig`].
    ///
    /// Checks the validity of the TLS configuration. Extend this method to add further validation as needed.
    ///
    /// # Errors
    /// Returns [`ValidationError`] if validation fails.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.tls.validate()
    }
}

/// Postgres connection settings for the source database.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SourcePgConnectionConfig {
    /// Host on which Postgres is running.
    host: String,
    /// Port on which Postgres is running.
    port: u16,
    /// Postgres database name.
    name: String,
    /// Postgres database user name.
    username: String,
    /// Postgres database user password. This field is sensitive.
    password: Option<String>,
    /// Postgres publication name.
    publication: String,
}

impl std::fmt::Debug for SourcePgConnectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourcePgConnectionConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("name", &self.name)
            .field("username", &self.username)
            .field("password", &"REDACTED")
            .field("publication", &self.publication)
            .finish()
    }
}

/// Supported destination configurations for the replicator.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationConfig {
    /// The in-memory destination.
    Memory,
    /// The BigQuery destination.
    BigQuery {
        /// BigQuery project id.
        project_id: String,
        /// BigQuery dataset id.
        dataset_id: String,
        /// BigQuery service account key.
        service_account_key: String,
        /// The max_staleness parameter for BigQuery:
        /// https://cloud.google.com/bigquery/docs/change-data-capture#create-max-staleness
        #[serde(skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>,
    },
}

impl std::fmt::Debug for DestinationConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DestinationConfig::Memory => f.write_str("Memory"),
            DestinationConfig::BigQuery {
                ..
            } => f.write_str("BigQuery"),
        }
    }
}

/// Batch processing configuration.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct BatchConfig {
    /// Maximum batch size for table copy and events stream.
    pub max_size: usize,
    /// Maximum duration, in seconds, to wait for a batch to fill.
    pub max_fill_secs: u64,
}

impl std::fmt::Debug for BatchConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchConfig")
            .field("max_size", &self.max_size)
            .field("max_fill_secs", &self.max_fill_secs)
            .finish()
    }
}

/// TLS configuration for secure connections.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TlsConfig {
    /// Trusted root certificates in PEM format. This field is sensitive.
    pub trusted_root_certs: String,
    /// True if TLS is enabled.
    pub enabled: bool,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field("trusted_root_certs", &"REDACTED")
            .field("enabled", &self.enabled)
            .finish()
    }
}

impl TlsConfig {
    /// Validates the [`TlsConfig`].
    ///
    /// Ensures that trusted root certificates are provided if TLS is enabled.
    ///
    /// # Errors
    /// Returns [`ValidationError::MissingTrustedRootCerts`] if TLS is enabled but no certificates are set.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.enabled && self.trusted_root_certs.is_empty() {
            return Err(ValidationError::MissingTrustedRootCerts);
        }
        Ok(())
    }
}