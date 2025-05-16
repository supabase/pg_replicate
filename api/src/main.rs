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
    let _log_flusher = init_tracing(app_name)?;
    let args: Vec<String> = env::args().collect();

    match args.len() {
        // Run the application server
        1 => {
            let configuration = get_settings::<'_, Settings>()?;
            log_database_details(&configuration.database);
            let application = Application::build(configuration.clone()).await?;
            application.run_until_stopped().await?;
        }
        // Handle single command commands
        2 => {
            let command = &args[1];
            match command.as_str() {
                "migrate" => {
                    let configuration = get_settings::<'_, DatabaseSettings>()?;
                    log_database_details(&configuration);
                    Application::migrate_database(configuration).await?;
                    info!("database migrated successfully");
                }
                _ => {
                    let message = format!("invalid command: {command}");
                    error!("{message}");
                    return Err(anyhow!(message));
                }
            }
        }
        _ => {
            let message = "invalid command line arguments";
            error!("{message}");
            return Err(anyhow!(message));
        }
    }

    Ok(())
}

fn log_database_details(settings: &DatabaseSettings) {
    let DatabaseSettings {
        host,
        port,
        name,
        username,
        password: _,
        require_ssl,
    } = settings;
    
    info!(
        host,
        port,
        dbname = name,
        username,
        require_ssl,
        "database details",
    );
}
