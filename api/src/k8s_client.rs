use k8s_openapi::api::core::v1::{ConfigMap, Pod, Secret};
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
    pods_api: Api<Pod>,
}

const SECRET_NAME_SUFFIX: &str = "bq-service-account-key";
const CONFIG_MAP_NAME_SUFFIX: &str = "replicator-config";
const POD_NAME_SUFFIX: &str = "replicator";

impl K8sClient {
    pub async fn new() -> Result<K8sClient, K8sError> {
        let client = Client::try_default().await?;

        let secrets_api: Api<Secret> = Api::default_namespaced(client.clone());
        let config_maps_api: Api<ConfigMap> = Api::default_namespaced(client.clone());
        let pods_api: Api<Pod> = Api::default_namespaced(client);

        Ok(K8sClient {
            secrets_api,
            config_maps_api,
            pods_api,
        })
    }

    pub async fn create_or_update_secret(
        &self,
        prefix: &str,
        bq_service_account_key: &str,
    ) -> Result<(), K8sError> {
        info!("patching secret");

        let secret_name = format!("{prefix}-{SECRET_NAME_SUFFIX}");
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
        info!("patched secret");

        Ok(())
    }

    pub async fn delete_secret(&self, prefix: &str) -> Result<(), K8sError> {
        info!("deleting secret");
        let secret_name = format!("{prefix}-{SECRET_NAME_SUFFIX}");
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
        info!("deleted secret");
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

    pub async fn create_or_update_pod(&self, prefix: &str) -> Result<(), K8sError> {
        info!("patching pod");

        let pod_name = format!("{prefix}-{POD_NAME_SUFFIX}");
        let secret_name = format!("{prefix}-{SECRET_NAME_SUFFIX}");
        let config_map_name = format!("{prefix}-{CONFIG_MAP_NAME_SUFFIX}");

        let pod_json = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": pod_name },
            "spec": {
                "volumes": [
                  {
                    "name": "config-file",
                    "configMap": {
                      "name": config_map_name
                    }
                  }
                ],
                "containers": [{
                  "name": "replicator",
                  "image": "ramsup/replicator:0.0.7",
                  "env": [
                    {
                      "name": "APP_ENVIRONMENT",
                      "value": "prod"
                    },
                    {
                      "name": "APP_SINK__BIGQUERY__SERVICE_ACCOUNT_KEY",
                      "valueFrom": {
                        "secretKeyRef": {
                          "name": secret_name,
                          "key": "service-account-key"
                        }
                      }
                    }
                  ],
                  "volumeMounts": [{
                    "name": "config-file",
                    "mountPath": "/app/configuration"
                  }]
                }],
            }
        });
        let pod: Pod = serde_json::from_value(pod_json)?;

        let pp = PatchParams::apply(&pod_name);
        self.pods_api
            .patch(&pod_name, &pp, &Patch::Apply(pod))
            .await?;
        info!("patched pod");
        Ok(())
    }

    pub async fn delete_pod(&self, prefix: &str) -> Result<(), K8sError> {
        info!("deleting Pod");
        let pod_name = format!("{prefix}-{POD_NAME_SUFFIX}");
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
        info!("deleted Pod");
        Ok(())
    }
}
