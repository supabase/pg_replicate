use std::env;

use anyhow::anyhow;
use api::{
    configuration::{get_settings, DatabaseSettings, Settings},
    startup::Application,
};
use telemetry::init_tracing;
use tracing::{error, info};

#[actix_web::main]
pub async fn main() -> anyhow::Result<()> {
    let app_name = env!("CARGO_BIN_NAME");
    // We pass emit_on_span_close = true to emit logs on span close
    // for the api because it is a web server and we need to emit logs
    // for every closing request. This is a bit of a hack, but it works
    // for now. Ideally the tracing middleware should emit a log on
    // request end, but it doesn't do that yet.
    let _log_flusher = init_tracing(app_name, true)?;
    let mut args = env::args();

    if args.len() == 2 {
        let command = args.nth(1).unwrap();
        if command == "migrate" {
            let configuration = get_settings::<'_, DatabaseSettings>()?;
            let DatabaseSettings {
                host,
                port,
                name,
                username,
                password: _,
                require_ssl,
            } = &configuration;
            info!(
                host,
                port,
                dbname = name,
                username,
                require_ssl,
                "database details",
            );
            Application::migrate_database(configuration).await?;
            info!("database migrated successfullly");
        } else if command == "delete-test-databases" {
            // The cargo test command generates a lot of test databases
            // and this command deletes them all.
            let configuration = get_settings::<'_, Settings>()?;
            let DatabaseSettings {
                host,
                port,
                name,
                username,
                password: _,
                require_ssl,
            } = &configuration.database;
            info!(
                host,
                port,
                dbname = name,
                username,
                require_ssl,
                "database details",
            );
            let num_deleted = Application::delete_all_test_databases(configuration).await?;
            info!("{num_deleted} test databases deleted successfullly");
        } else {
            let message = "invalid command: {command}";
            error!("{message}");
            return Err(anyhow!(message));
        }
    } else if args.len() == 1 {
        let configuration = get_settings::<'_, Settings>()?;
        let DatabaseSettings {
            host,
            port,
            name,
            username,
            password: _,
            require_ssl,
        } = &configuration.database;
        info!(
            host,
            port,
            dbname = name,
            username,
            require_ssl,
            "database details",
        );
        let application = Application::build(configuration.clone()).await?;
        application.run_until_stopped().await?;
    } else {
        let message = "invalid command line arguments";
        error!("{message}");
        return Err(anyhow!(message));
    }

    Ok(())
}
