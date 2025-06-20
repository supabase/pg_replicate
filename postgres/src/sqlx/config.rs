use secrecy::{ExposeSecret, Secret};
use serde::Deserialize;
use sqlx::postgres::{PgConnectOptions, PgSslMode};

/// Connection config for a PostgreSQL database to be used with `sqlx`.
///
/// Contains the connection parameters needed to establish a connection to a PostgreSQL
/// database server, including network location, authentication credentials, and security
/// settings.
#[derive(Debug, Clone, Deserialize)]
pub struct PgConnectionConfig {
    /// Host name or IP address of the PostgreSQL server
    pub host: String,
    /// Port number that the PostgreSQL server listens on
    pub port: u16,
    /// Name of the target database
    pub name: String,
    /// Username for authentication
    pub username: String,
    /// Optional password for authentication, wrapped in [`Secret`] for secure handling
    pub password: Option<Secret<String>>,
    /// If true, requires SSL/TLS encryption for the connection
    pub require_ssl: bool,
}

impl PgConnectionConfig {
    /// Creates connection options for connecting to the PostgreSQL server without
    /// specifying a database.
    ///
    /// Returns [`PgConnectOptions`] configured with the host, port, username, SSL mode
    /// and optional password from this instance. Useful for administrative operations
    /// that must be performed before connecting to a specific database, like database
    /// creation.
    pub fn without_db(&self) -> PgConnectOptions {
        // TODO: explore the possibility to support for certificates.
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

    /// Creates connection options for connecting to a specific database.
    ///
    /// Returns [`PgConnectOptions`] configured with all connection parameters including
    /// the database name from this instance.
    pub fn with_db(&self) -> PgConnectOptions {
        self.without_db().database(&self.name)
    }
}
