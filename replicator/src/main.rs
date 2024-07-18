use std::error::Error;

use configuration::get_configuration;
use tracing::{error, info};

mod configuration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(e) = main_impl().await {
        error!("{e}");
    }

    Ok(())
}

async fn main_impl() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let settings = get_configuration()?;

    info!("settings: {settings:#?}");

    Ok(())
}
