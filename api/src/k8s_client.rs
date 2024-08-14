use k8s_openapi::api::{
    apps::v1::StatefulSet,
    core::v1::{ConfigMap, Pod, Secret},
};
use serde_json::json;
use thiserror::Error;
use tracing::*;

use kube::{
    api::{Api, DeleteParams, Patch, PatchParams},
    Client,
};

#[derive(Debug, Error)]
pub enum K8sError {
    #[error["serde_json error: {0}"]]
    Serde(#[from] serde_json::error::Error),

    #[error["kube error: {0}"]]
    Kube(#[from] kube::Error),
}

pub struct K8sClient {
    secrets_api: Api<Secret>,
    config_maps_api: Api<ConfigMap>,
    stateful_sets_api: Api<StatefulSet>,
    pods_api: Api<Pod>,
}

const BQ_SECRET_NAME_SUFFIX: &str = "bq-service-account-key";
const POSTGRES_SECRET_NAME_SUFFIX: &str = "postgres-password";
const CONFIG_MAP_NAME_SUFFIX: &str = "replicator-config";
const STATEFUL_SET_NAME_SUFFIX: &str = "replicator";
const CONTAINER_NAME_SUFFIX: &str = "replicator";

impl K8sClient {
    pub async fn new() -> Result<K8sClient, K8sError> {
        let client = Client::try_default().await?;

        let secrets_api: Api<Secret> = Api::default_namespaced(client.clone());
        let config_maps_api: Api<ConfigMap> = Api::default_namespaced(client.clone());
        let stateful_sets_api: Api<StatefulSet> = Api::default_namespaced(client.clone());
        let pods_api: Api<Pod> = Api::default_namespaced(client);

        Ok(K8sClient {
            secrets_api,
            config_maps_api,
            stateful_sets_api,
            pods_api,
        })
    }

    pub async fn create_or_update_postgres_secret(
        &self,
        prefix: &str,
        postgres_password: &str,
    ) -> Result<(), K8sError> {
        info!("patching postgres secret");

        let secret_name = format!("{prefix}-{POSTGRES_SECRET_NAME_SUFFIX}");
        let secret_json = json!({
          "apiVersion": "v1",
          "kind": "Secret",
          "metadata": {
            "name": secret_name
          },
          "type": "Opaque",
          "stringData": {
            "password": postgres_password,
          }
        });
        let secret: Secret = serde_json::from_value(secret_json)?;

        let pp = PatchParams::apply(&secret_name);
        self.secrets_api
            .patch(&secret_name, &pp, &Patch::Apply(secret))
            .await?;
        info!("patched postgres secret");

        Ok(())
    }

    pub async fn create_or_update_bq_secret(
        &self,
        prefix: &str,
        bq_service_account_key: &str,
    ) -> Result<(), K8sError> {
        info!("patching bq secret");

        let secret_name = format!("{prefix}-{BQ_SECRET_NAME_SUFFIX}");
        let secret_json = json!({
          "apiVersion": "v1",
          "kind": "Secret",
          "metadata": {
            "name": secret_name
          },
          "type": "Opaque",
          "stringData": {
            "service-account-key": bq_service_account_key,
          }
        });
        let secret: Secret = serde_json::from_value(secret_json)?;

        let pp = PatchParams::apply(&secret_name);
        self.secrets_api
            .patch(&secret_name, &pp, &Patch::Apply(secret))
            .await?;
        info!("patched bq secret");

        Ok(())
    }

    pub async fn delete_postgres_secret(&self, prefix: &str) -> Result<(), K8sError> {
        info!("deleting postgres secret");
        let secret_name = format!("{prefix}-{POSTGRES_SECRET_NAME_SUFFIX}");
        let dp = DeleteParams::default();
        match self.secrets_api.delete(&secret_name, &dp).await {
            Ok(_) => {}
            Err(e) => match e {
                kube::Error::Api(ref er) => {
                    if er.code != 404 {
                        return Err(e.into());
                    }
                }
                e => return Err(e.into()),
            },
        }
        info!("deleted postgres secret");
        Ok(())
    }

    pub async fn delete_bq_secret(&self, prefix: &str) -> Result<(), K8sError> {
        info!("deleting bq secret");
        let secret_name = format!("{prefix}-{BQ_SECRET_NAME_SUFFIX}");
        let dp = DeleteParams::default();
        match self.secrets_api.delete(&secret_name, &dp).await {
            Ok(_) => {}
            Err(e) => match e {
                kube::Error::Api(ref er) => {
                    if er.code != 404 {
                        return Err(e.into());
                    }
                }
                e => return Err(e.into()),
            },
        }
        info!("deleted bq secret");
        Ok(())
    }

    pub async fn create_or_update_config_map(
        &self,
        prefix: &str,
        base_config: &str,
        prod_config: &str,
    ) -> Result<(), K8sError> {
        info!("patching config map");

        let config_map_name = format!("{prefix}-{CONFIG_MAP_NAME_SUFFIX}");
        let config_map_json = json!({
          "kind": "ConfigMap",
          "apiVersion": "v1",
          "metadata": {
            "name": config_map_name
          },
          "data": {
            "base.yaml": base_config,
            "prod.yaml": prod_config,
          }
        });
        let config_map: ConfigMap = serde_json::from_value(config_map_json)?;

        let pp = PatchParams::apply(&config_map_name);
        self.config_maps_api
            .patch(&config_map_name, &pp, &Patch::Apply(config_map))
            .await?;
        info!("patched config map");
        Ok(())
    }

    pub async fn delete_config_map(&self, prefix: &str) -> Result<(), K8sError> {
        info!("deleting config map");
        let config_map_name = format!("{prefix}-{CONFIG_MAP_NAME_SUFFIX}");
        let dp = DeleteParams::default();
        match self.config_maps_api.delete(&config_map_name, &dp).await {
            Ok(_) => {}
            Err(e) => match e {
                kube::Error::Api(ref er) => {
                    if er.code != 404 {
                        return Err(e.into());
                    }
                }
                e => return Err(e.into()),
            },
        }
        info!("deleted config map");
        Ok(())
    }

    pub async fn create_or_update_stateful_set(
        &self,
        prefix: &str,
        replicator_image: &str,
    ) -> Result<(), K8sError> {
        info!("patching stateful set");

        let stateful_set_name = format!("{prefix}-{STATEFUL_SET_NAME_SUFFIX}");
        let container_name = format!("{prefix}-{CONTAINER_NAME_SUFFIX}");
        let postgres_secret_name = format!("{prefix}-{POSTGRES_SECRET_NAME_SUFFIX}");
        let bq_secret_name = format!("{prefix}-{BQ_SECRET_NAME_SUFFIX}");
        let config_map_name = format!("{prefix}-{CONFIG_MAP_NAME_SUFFIX}");

        let stateful_set_json = json!({
          "apiVersion": "apps/v1",
          "kind": "StatefulSet",
          "metadata": {
            "name": stateful_set_name,
          },
          "spec": {
            "replicas": 1,
            "selector": {
              "matchLabels": {
                "app": stateful_set_name
              }
            },
            "template": {
              "metadata": {
                "labels": {
                  "app": stateful_set_name
                }
              },
              "spec": {
                "volumes": [
                  {
                    "name": "config-file",
                    "configMap": {
                      "name": config_map_name
                    }
                  }
                ],
                "containers": [
                  {
                    "name": container_name,
                    "image": replicator_image,
                    "env": [
                      {
                        "name": "APP_ENVIRONMENT",
                        "value": "prod"
                      },
                      {
                        "name": "APP_SOURCE__POSTGRES__PASSWORD",
                        "valueFrom": {
                          "secretKeyRef": {
                            "name": postgres_secret_name,
                            "key": "password"
                          }
                        }
                      },
                      {
                        "name": "APP_SINK__BIGQUERY__SERVICE_ACCOUNT_KEY",
                        "valueFrom": {
                          "secretKeyRef": {
                            "name": bq_secret_name,
                            "key": "service-account-key"
                          }
                        }
                      }
                    ],
                    "volumeMounts": [{
                      "name": "config-file",
                      "mountPath": "/app/configuration"
                    }]
                  }
                ]
              }
            }
          }
        });

        let stateful_set: StatefulSet = serde_json::from_value(stateful_set_json)?;

        let pp = PatchParams::apply(&stateful_set_name);
        self.stateful_sets_api
            .patch(&stateful_set_name, &pp, &Patch::Apply(stateful_set))
            .await?;

        self.delete_pod(prefix).await?;

        info!("patched stateful set");

        Ok(())
    }

    pub async fn delete_stateful_set(&self, prefix: &str) -> Result<(), K8sError> {
        info!("deleting stateful set");
        let stateful_set_name = format!("{prefix}-{STATEFUL_SET_NAME_SUFFIX}");
        let dp = DeleteParams::default();
        match self.stateful_sets_api.delete(&stateful_set_name, &dp).await {
            Ok(_) => {}
            Err(e) => match e {
                kube::Error::Api(ref er) => {
                    if er.code != 404 {
                        return Err(e.into());
                    }
                }
                e => return Err(e.into()),
            },
        }
        self.delete_pod(prefix).await?;
        info!("deleted stateful set");
        Ok(())
    }

    pub async fn delete_pod(&self, prefix: &str) -> Result<(), K8sError> {
        info!("deleting pod");
        let pod_name = format!("{prefix}-{STATEFUL_SET_NAME_SUFFIX}-0");
        let dp = DeleteParams::default();
        match self.pods_api.delete(&pod_name, &dp).await {
            Ok(_) => {}
            Err(e) => match e {
                kube::Error::Api(ref er) => {
                    if er.code != 404 {
                        return Err(e.into());
                    }
                }
                e => return Err(e.into()),
            },
        }
        info!("deleted pod");
        Ok(())
    }
}
