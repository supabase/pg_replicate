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

pub async fn run_worker_until_stopped<C: K8sClient>(
    configuration: Settings,
    k8s_client: C,
) -> Result<(), anyhow::Error> {
    let connection_pool = get_connection_pool(&configuration.database);
    let poll_duration = Duration::from_secs(configuration.worker.poll_interval_secs);
    worker_loop(connection_pool, poll_duration, &k8s_client).await
}

async fn worker_loop<C: K8sClient>(
    pool: PgPool,
    poll_duration: Duration,
    k8s_client: &C,
) -> Result<(), anyhow::Error> {
    loop {
        match try_execute_task(&pool, k8s_client).await {
            Ok(ExecutionOutcome::EmptyQueue) => {
                debug!("no task in queue");
                tokio::time::sleep(poll_duration).await;
            }
            Ok(ExecutionOutcome::TaskCompleted) => {
                debug!("successfully executed task");
            }
            Err(e) => {
                error!("error while executing task: {e:#?}");
                tokio::time::sleep(poll_duration).await;
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Secrets {
    pub postgres_password: String,
    pub bigquery_service_account_key: String,
}

#[allow(clippy::large_enum_variant)]
#[derive(serde::Serialize, serde::Deserialize)]
pub enum Request {
    CreateOrUpdateSecrets {
        tenant_id: String,
        replicator_id: i64,
        secrets: Secrets,
    },
    CreateOrUpdateConfig {
        tenant_id: String,
        replicator_id: i64,
        config: replicator_config::Config,
    },
    CreateOrUpdateReplicator {
        tenant_id: String,
        replicator_id: i64,
        replicator_image: String,
    },
    DeleteSecrets {
        tenant_id: String,
        replicator_id: i64,
    },
    DeleteConfig {
        tenant_id: String,
        replicator_id: i64,
    },
    DeleteReplicator {
        tenant_id: String,
        replicator_id: i64,
    },
}

fn create_prefix(tenant_id: &str, replicator_id: i64) -> String {
    format!("{tenant_id}-{replicator_id}")
}

pub async fn try_execute_task<C: K8sClient>(
    pool: &PgPool,
    k8s_client: &C,
) -> Result<ExecutionOutcome, anyhow::Error> {
    let task = dequeue_task(pool).await?;
    let Some((transaction, task)) = task else {
        return Ok(ExecutionOutcome::EmptyQueue);
    };

    let request = serde_json::from_value::<Request>(task.data)?;

    match request {
        Request::CreateOrUpdateSecrets {
            tenant_id,
            replicator_id,
            secrets,
        } => {
            info!("creating secrets for tenant_id: {tenant_id}, replicator_id: {replicator_id}");
            let Secrets {
                postgres_password,
                bigquery_service_account_key,
            } = secrets;
            let prefix = create_prefix(&tenant_id, replicator_id);
            k8s_client
                .create_or_update_postgres_secret(&prefix, &postgres_password)
                .await?;
            k8s_client
                .create_or_update_bq_secret(&prefix, &bigquery_service_account_key)
                .await?;
        }
        Request::CreateOrUpdateConfig {
            tenant_id,
            replicator_id,
            config,
        } => {
            info!("creating config map for tenant_id: {tenant_id}, replicator_id: {replicator_id}");
            let base_config = "";
            let prod_config = serde_json::to_string(&config)?;
            let prefix = create_prefix(&tenant_id, replicator_id);
            k8s_client
                .create_or_update_config_map(&prefix, base_config, &prod_config)
                .await?;
        }
        Request::CreateOrUpdateReplicator {
            tenant_id,
            replicator_id,
            replicator_image,
        } => {
            info!(
                "creating or updating stateful set for tenant_id: {tenant_id}, replicator_id: {replicator_id}"
            );

            let prefix = create_prefix(&tenant_id, replicator_id);
            k8s_client
                .create_or_update_stateful_set(&prefix, &replicator_image)
                .await?;
        }
        Request::DeleteSecrets {
            tenant_id,
            replicator_id,
        } => {
            info!("deleting secrets for tenant_id: {tenant_id}, replicator_id: {replicator_id}");
            let prefix = create_prefix(&tenant_id, replicator_id);
            k8s_client.delete_postgres_secret(&prefix).await?;
            k8s_client.delete_bq_secret(&prefix).await?;
        }
        Request::DeleteConfig {
            tenant_id,
            replicator_id,
        } => {
            info!("deleting config map for tenant_id: {tenant_id}, replicator_id: {replicator_id}");
            let prefix = create_prefix(&tenant_id, replicator_id);
            k8s_client.delete_config_map(&prefix).await?;
        }
        Request::DeleteReplicator {
            tenant_id,
            replicator_id,
        } => {
            info!(
                "deleting stateful set for tenant_id: {tenant_id}, replicator_id: {replicator_id}"
            );
            let prefix = create_prefix(&tenant_id, replicator_id);
            k8s_client.delete_stateful_set(&prefix).await?;
        }
    }

    delete_task(transaction, task.id).await?;

    Ok(ExecutionOutcome::TaskCompleted)
}

pub enum ExecutionOutcome {
    TaskCompleted,
    EmptyQueue,
}
