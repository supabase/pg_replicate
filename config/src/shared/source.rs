use serde::{Deserialize, Serialize};
use std::fmt;

use crate::shared::ValidationError;

/// Configuration for connecting to a Postgres source database.
///
/// This struct holds all necessary connection parameters and settings.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SourceConfig {
    /// Hostname or IP address of the Postgres server.
    pub host: String,
    /// Port number on which the Postgres server is listening.
    pub port: u16,
    /// Name of the Postgres database to connect to.
    pub name: String,
    /// Username for authenticating with the Postgres server.
    pub username: String,
    /// Password for the specified user. This field is sensitive and redacted in debug output.
    pub password: Option<String>,
    /// TLS configuration for secure connections.
    pub tls: TlsConfig,
}

impl fmt::Debug for SourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("name", &self.name)
            .field("username", &self.username)
            .field("password", &"REDACTED")
            .field("tls", &self.tls)
            .finish()
    }
}

/// TLS settings for secure Postgres connections.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TlsConfig {
    /// PEM-encoded trusted root certificates. Sensitive and redacted in debug output.
    pub trusted_root_certs: String,
    /// Whether TLS is enabled for the connection.
    pub enabled: bool,
}

impl fmt::Debug for TlsConfig {
    /// Formats the [`TlsConfig`] for debugging, redacting sensitive fields.
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
    /// If [`TlsConfig::enabled`] is true, this method checks that [`TlsConfig::trusted_root_certs`] is not empty.
    ///
    /// Returns [`ValidationError::MissingTrustedRootCerts`] if TLS is enabled but no certificates are provided.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.enabled && self.trusted_root_certs.is_empty() {
            return Err(ValidationError::MissingTrustedRootCerts);
        }

        Ok(())
    }
}
