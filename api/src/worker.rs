use std::time::Duration;

use sqlx::PgPool;

use crate::{
    configuration::Settings,
    queue::{delete_task, dequeue_task},
    startup::get_connection_pool,
};

pub async fn run_worker_until_stopped(configuration: Settings) -> Result<(), anyhow::Error> {
    let connection_pool = get_connection_pool(&configuration.database);
    worker_loop(connection_pool).await
}

async fn worker_loop(pool: PgPool) -> Result<(), anyhow::Error> {
    loop {
        match try_execute_task(&pool).await {
            Ok(ExecutionOutcome::EmptyQueue) => {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
            Err(_) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(ExecutionOutcome::TaskCompleted) => {}
        }
    }
}

pub async fn try_execute_task(pool: &PgPool) -> Result<ExecutionOutcome, anyhow::Error> {
    let task = dequeue_task(pool).await?;
    if task.is_none() {
        return Ok(ExecutionOutcome::EmptyQueue);
    }
    let (transaction, task) = task.unwrap();
    //TODO: perform task here
    delete_task(transaction, task.id).await?;
    Ok(ExecutionOutcome::TaskCompleted)
}

pub enum ExecutionOutcome {
    TaskCompleted,
    EmptyQueue,
}
