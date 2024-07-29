use std::error::Error;

use queue::enqueue;
use serde_json::json;
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

    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5431 dbname=replicator user=raminder.singh",
        NoTls,
    )
    .await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let id = enqueue(&client, "test task", json!({"key": "value"})).await?;

    info!("task with id {id} enqueued in pending state");

    Ok(())
}
