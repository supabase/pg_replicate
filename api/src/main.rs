use std::env;

use anyhow::anyhow;
use api::{
    configuration::{get_settings, Settings},
    startup::Application,
};
use postgres::options::PgDatabaseOptions;
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

    match args.len() {
        // Run the application server
        1 => {
            let configuration = get_settings::<'_, Settings>()?;
            log_pg_database_options(&configuration.database);
            let application = Application::build(configuration.clone()).await?;
            application.run_until_stopped().await?;
        }
        // Handle single command commands
        2 => {
            let command = args.nth(1).unwrap();
            match command.as_str() {
                "migrate" => {
                    let options = get_settings::<'_, PgDatabaseOptions>()?;
                    log_pg_database_options(&options);
                    Application::migrate_database(options).await?;
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
            let message = "invalid number of command line arguments";
            error!("{message}");
            return Err(anyhow!(message));
        }
    }

    Ok(())
}

fn log_pg_database_options(options: &PgDatabaseOptions) {
    info!(
        host = options.host,
        port = options.port,
        dbname = options.name,
        username = options.username,
        require_ssl = options.require_ssl,
        "pg database options",
    );
}
