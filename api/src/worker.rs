use std::time::Duration;

use config_types::SinkSettings;
use sqlx::PgPool;
use tracing::{debug, info};
use tracing_log::log::error;

use crate::{
    configuration::Settings,
    k8s_client::K8sClient,
    queue::{delete_task, dequeue_task},
    startup::get_connection_pool,
};

pub async fn run_worker_until_stopped(configuration: Settings) -> Result<(), anyhow::Error> {
    let connection_pool = get_connection_pool(&configuration.database);
    let poll_duration = Duration::from_secs(configuration.worker.poll_interval_secs);
    worker_loop(connection_pool, poll_duration).await
}

async fn worker_loop(pool: PgPool, poll_duration: Duration) -> Result<(), anyhow::Error> {
    let k8s_client = K8sClient::new().await?;
    loop {
        match try_execute_task(&pool, &k8s_client).await {
            Ok(ExecutionOutcome::EmptyQueue) => {
                debug!("no task in queue");
            }
            Ok(ExecutionOutcome::TaskCompleted) => {
                debug!("successfully executed task");
            }
            Err(e) => {
                error!("error while executing task: {e:#?}");
            }
        }
        tokio::time::sleep(poll_duration).await;
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(serde::Serialize, serde::Deserialize)]
pub enum Request {
    CreateOrUpdate {
        project_ref: String,
        settings: config_types::Settings,
    },
    Delete {
        project_ref: String,
    },
}

pub async fn try_execute_task(
    pool: &PgPool,
    k8s_client: &K8sClient,
) -> Result<ExecutionOutcome, anyhow::Error> {
    let task = dequeue_task(pool).await?;
    let Some((transaction, task)) = task else {
        return Ok(ExecutionOutcome::EmptyQueue);
    };

    let request = serde_json::from_value::<Request>(task.data)?;

    match request {
        Request::CreateOrUpdate {
            project_ref,
            settings,
        } => {
            info!(
                "creating or updating k8s objects for project ref: {}",
                project_ref
            );

            let SinkSettings::BigQuery {
                project_id: _,
                dataset_id: _,
                service_account_key,
            } = &settings.sink;
            k8s_client
                .create_or_update_secret(service_account_key)
                .await?;
            let base_config = "";
            let prod_config = serde_json::to_string(&settings)?;
            k8s_client
                .create_or_update_config_map(base_config, &prod_config)
                .await?;
            k8s_client.create_or_update_pod().await?;
        }
        Request::Delete { project_ref } => {
            info!("deleting project ref: {}", project_ref);
            k8s_client.delete_pod().await?;
            k8s_client.delete_config_map().await?;
            k8s_client.delete_secret().await?;
        }
    }

    delete_task(transaction, task.id).await?;

    Ok(ExecutionOutcome::TaskCompleted)
}

pub enum ExecutionOutcome {
    TaskCompleted,
    EmptyQueue,
}
