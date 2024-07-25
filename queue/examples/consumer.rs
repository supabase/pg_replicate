use std::error::Error;

use queue::dequeue;
use tokio_postgres::NoTls;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(e) = main_impl().await {
        error!("{e}");
    }

    Ok(())
}

async fn main_impl() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let (mut client, connection) = tokio_postgres::connect(
        "host=localhost port=5431 dbname=replicator user=raminder.singh",
        NoTls,
    )
    .await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let tasks = dequeue(&mut client, 2).await?;

    info!("tasks dequeued: {tasks:#?}");

    Ok(())
}
