use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Errors that can occur while loading the [`ReplicatorConfig`].
#[derive(Debug, Error)]
pub enum ReplicatorConfigError {
    /// Validation error.
    #[error("An error occurred while validating the loaded configuration: {0}")]
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
    /// The unique identifier for the pipeline.
    pub pipeline_id: u64,
    /// Source Postgres connection settings.
    pub source: SourcePgConnectionConfig,
    /// State store configuration.
    pub state_store: StateStoreConfig,
    /// Destination configuration.
    pub destination: DestinationConfig,
    /// Batch processing configuration.
    pub batch: BatchConfig,
    /// TLS configuration.
    pub tls: TlsConfig,
    /// Supabase specific configuration.
    pub supabase: SupabaseConfig,
}

impl fmt::Debug for ReplicatorConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplicatorConfig")
            .field("pipeline_id", &self.pipeline_id)
            .field("source", &self.source)
            .field("state_store", &self.state_store)
            .field("destination", &self.destination)
            .field("batch", &self.batch)
            .field("tls", &self.tls)
            .field("supabase", &self.supabase)
            .finish()
    }
}

impl ReplicatorConfig {
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
    pub host: String,
    /// Port on which Postgres is running.
    pub port: u16,
    /// Postgres database name.
    pub name: String,
    /// Postgres database user name.
    pub username: String,
    /// Postgres database user password. This field is sensitive.
    pub password: Option<String>,
    /// Postgres publication name.
    pub publication: String,
}

impl fmt::Debug for SourcePgConnectionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateStoreConfig {
    /// The in-memory state store.
    Memory,
}

impl fmt::Debug for StateStoreConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => write!(f, "Memory"),
        }
    }
}

impl Default for StateStoreConfig {
    fn default() -> Self {
        Self::Memory
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
        /// The `max_staleness` parameter for BigQuery:
        /// https://cloud.google.com/bigquery/docs/change-data-capture#create-max-staleness
        #[serde(skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>,
    },
}

impl fmt::Debug for DestinationConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => f.write_str("Memory"),
            Self::BigQuery { .. } => f.write_str("BigQuery"),
        }
    }
}

impl Default for DestinationConfig {
    fn default() -> Self {
        Self::Memory
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

impl fmt::Debug for BatchConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

impl fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SupabaseConfig {
    /// The project to which this replicator belongs.
    pub project: String,
}

impl fmt::Debug for SupabaseConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SupabaseConfig")
            .field("project", &self.project)
            .finish()
    }
}
