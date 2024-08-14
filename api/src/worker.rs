use std::time::Duration;

use sqlx::PgPool;
use tracing::{debug, info};
use tracing_log::log::error;

use crate::{
    configuration::Settings,
    k8s_client::K8sClient,
    queue::{delete_task, dequeue_task},
    replicator_config,
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

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Secrets {
    postgres_password: String,
    bigquery_service_account_key: String,
}

#[allow(clippy::large_enum_variant)]
#[derive(serde::Serialize, serde::Deserialize)]
pub enum Request {
    CreateOrUpdateSecrets {
        project_ref: String,
        secrets: Secrets,
    },
    CreateOrUpdateConfig {
        project_ref: String,
        config: replicator_config::Config,
    },
    CreateOrUpdateReplicator {
        project_ref: String,
        replicator_image: String,
    },
    DeleteSecrets {
        project_ref: String,
    },
    DeleteConfig {
        project_ref: String,
    },
    DeleteReplicator {
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
        Request::CreateOrUpdateSecrets {
            project_ref,
            secrets,
        } => {
            info!("creating secrets for project ref: {}", project_ref);
            let Secrets {
                postgres_password,
                bigquery_service_account_key,
            } = secrets;
            k8s_client
                .create_or_update_postgres_secret(&project_ref, &postgres_password)
                .await?;
            k8s_client
                .create_or_update_bq_secret(&project_ref, &bigquery_service_account_key)
                .await?;
        }
        Request::CreateOrUpdateConfig {
            project_ref,
            config: settings,
        } => {
            info!("creating config map for project ref: {}", project_ref);
            let base_config = "";
            let prod_config = serde_json::to_string(&settings)?;
            k8s_client
                .create_or_update_config_map(&project_ref, base_config, &prod_config)
                .await?;
        }
        Request::CreateOrUpdateReplicator {
            project_ref,
            replicator_image,
        } => {
            info!(
                "creating or updating stateful set for project ref: {}",
                project_ref
            );

            k8s_client
                .create_or_update_stateful_set(&project_ref, &replicator_image)
                .await?;
        }
        Request::DeleteSecrets { project_ref } => {
            info!("deleting secrets for project ref: {}", project_ref);
            k8s_client.delete_postgres_secret(&project_ref).await?;
            k8s_client.delete_bq_secret(&project_ref).await?;
        }
        Request::DeleteConfig { project_ref } => {
            info!("deleting config map for project ref: {}", project_ref);
            k8s_client.delete_config_map(&project_ref).await?;
        }
        Request::DeleteReplicator { project_ref } => {
            info!("deleting stateful set for project ref: {}", project_ref);
            k8s_client.delete_stateful_set(&project_ref).await?;
        }
    }

    delete_task(transaction, task.id).await?;

    Ok(ExecutionOutcome::TaskCompleted)
}

pub enum ExecutionOutcome {
    TaskCompleted,
    EmptyQueue,
}
