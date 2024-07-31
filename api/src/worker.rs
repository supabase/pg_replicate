use std::time::Duration;

use sqlx::PgPool;
use tracing::debug;
use tracing_log::log::error;

use crate::{
    configuration::Settings,
    queue::{delete_task, dequeue_task},
    startup::get_connection_pool,
};

pub async fn run_worker_until_stopped(configuration: Settings) -> Result<(), anyhow::Error> {
    let connection_pool = get_connection_pool(&configuration.database);
    let poll_duration = Duration::from_secs(configuration.worker.poll_interval_secs);
    worker_loop(connection_pool, poll_duration).await
}

async fn worker_loop(pool: PgPool, poll_duration: Duration) -> Result<(), anyhow::Error> {
    loop {
        match try_execute_task(&pool).await {
            Ok(ExecutionOutcome::EmptyQueue) => {
                debug!("no task in queue");
            }
            Ok(ExecutionOutcome::TaskCompleted) => {
                debug!("successfully executed task");
            }
            Err(e) => {
                error!("error while executing task: {e}");
            }
        }
        tokio::time::sleep(poll_duration).await;
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
