use std::env;

use anyhow::anyhow;
use api::{
    configuration::{get_settings, DatabaseSettings, Settings},
    startup::Application,
    telemetry::init_tracing,
};
use tracing::info;
use tracing_log::log::error;

#[actix_web::main]
pub async fn main() -> anyhow::Result<()> {
    let _log_flusher = init_tracing()?;
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
            info!("database: host: {host}, port: {port}, dbname: {name}, username: {username}, require_ssl: {require_ssl}");
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
            info!("database: host: {host}, port: {port}, dbname: {name}, username: {username}, require_ssl: {require_ssl}");
            let num_deleted = Application::delete_all_test_databases(configuration).await?;
            info!("{num_deleted} test databases deleted successfullly");
        } else {
            let message = "invalid command line arguments";
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
        info!("database: host: {host}, port: {port}, dbname: {name}, username: {username}, require_ssl: {require_ssl}");
        let application = Application::build(configuration.clone()).await?;
        application.run_until_stopped().await?;
    } else {
        let message = "invalid command line arguments";
        error!("{message}");
        return Err(anyhow!(message));
    }

    Ok(())
}
