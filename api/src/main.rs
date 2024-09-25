use std::env;

use anyhow::anyhow;
use api::{
    configuration::get_configuration,
    startup::Application,
    telemetry::{get_subscriber, init_subscriber},
};
use tracing::info;
use tracing_log::log::error;

#[actix_web::main]
pub async fn main() -> anyhow::Result<()> {
    let subscriber = get_subscriber("api".into(), "info".into(), std::io::stdout);
    init_subscriber(subscriber);
    let configuration = get_configuration().expect("Failed to read configuration.");
    info!("{configuration}");
    let mut args = env::args();

    if args.len() == 2 {
        let command = args.nth(1).unwrap();
        if command == "migrate" {
            Application::migrate_database(configuration).await?;
            info!("database migrated successfullly");
        } else {
            let message = "invalid command line arguments";
            error!("{message}");
            return Err(anyhow!(message));
        }
    } else if args.len() == 1 {
        let application = Application::build(configuration.clone()).await?;
        application.run_until_stopped().await?;
    } else {
        let message = "invalid command line arguments";
        error!("{message}");
        return Err(anyhow!(message));
    }

    Ok(())
}
