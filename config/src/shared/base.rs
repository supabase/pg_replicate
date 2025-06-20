use thiserror::Error;

/// Errors that can occur during configuration validation.
#[derive(Debug, Error)]
pub enum ValidationError {
    /// TLS is enabled but no trusted root certificates are provided.
    #[error("Invalid TLS config: `trusted_root_certs` must be set when `enabled` is true")]
    MissingTrustedRootCerts,
}
