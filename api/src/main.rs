use std::{error::Error, net::TcpListener};

use api::{configuration::get_configuration, run};

#[actix_web::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let configuration = get_configuration()?;
    let listen_address = format!("127.0.0.1:{}", configuration.application_port);
    let connection_pool = api::get_connection_pool(&configuration.database);
    let listener = TcpListener::bind(listen_address)?;

    run(listener, connection_pool)?.await?;

    Ok(())
}
