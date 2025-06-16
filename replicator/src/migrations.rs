use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
    Executor,
};

use crate::configuration::{SourceSettings, TlsSettings};

pub async fn migrate_source_database(
    source_settings: &SourceSettings,
    tls_settings: &TlsSettings,
) -> Result<(), sqlx::Error> {
    let SourceSettings::Postgres {
        host,
        port,
        name,
        username,
        password,
        slot_name: _,
        publication: _,
    } = &source_settings;
    let options = PgConnectOptions::new()
        .application_name("replicator_migrator")
        .host(host)
        .port(*port)
        .username(username)
        .database(name);
    let options = if let Some(password) = password {
        options.password(password)
    } else {
        options
    };
    let TlsSettings {
        trusted_root_certs,
        enabled: tls_enabled,
    } = &tls_settings;

    let options = if *tls_enabled {
        options
            .ssl_root_cert_from_pem(trusted_root_certs.as_bytes().to_vec())
            .ssl_mode(PgSslMode::VerifyFull)
    } else {
        options
    };

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                // We set the search_path to  replication so that the _sqlx_migrations
                // metadata table is created inside that schema instead of the public
                // schema
                conn.execute("set search_path = 'replication';").await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await?;

    let migrator = sqlx::migrate!("./migrations");
    migrator.run(&pool).await?;

    Ok(())
}
