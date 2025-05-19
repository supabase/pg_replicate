use tokio_postgres::Config;
use tokio_postgres::config::SslMode;

#[derive(Debug, Clone)]
pub struct PgDatabaseOptions {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub name: String,
    pub username: String,
    pub password: Option<String>,
    pub ssl_mode: Option<SslMode>,
}

impl PgDatabaseOptions {
    pub fn without_db(&self) -> Config {
        let mut this = self.clone();
        // Postgres requires a database, so we default to the database which is equal to the username
        // since this seems to be the standard.
        this.database = this.username.clone();

        this.into()
    }

    pub fn with_db(&self) -> Config {
        self.clone().into()
    }
}

impl From<PgDatabaseOptions> for Config {
    fn from(value: PgDatabaseOptions) -> Self {
        let mut config = Config::new();
        config
            .host(value.host)
            .port(value.port)
            .dbname(value.database)
            .user(value.username);

        if let Some(password) = value.password {
            config.password(password);
        }

        if let Some(ssl_mode) = value.ssl_mode {
            config.ssl_mode(ssl_mode);
        }

        config
    }
}
