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

const SECRET_NAME: &str = "bq-service-account-key";
const CONFIG_MAP_NAME: &str = "replicator-config";
const POD_NAME: &str = "replicator";

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
        bq_service_account_key: &str,
    ) -> Result<(), K8sError> {
        info!("patching secret");

        let secret_json = json!({
          "apiVersion": "v1",
          "kind": "Secret",
          "metadata": {
            "name": SECRET_NAME
          },
          "type": "Opaque",
          "stringData": {
            "service-account-key": bq_service_account_key,
          }
        });
        let secret: Secret = serde_json::from_value(secret_json)?;

        let pp = PatchParams::apply(SECRET_NAME);
        self.secrets_api
            .patch(SECRET_NAME, &pp, &Patch::Apply(secret))
            .await?;
        info!("patched secret");

        Ok(())
    }

    pub async fn delete_secret(&self) -> Result<(), K8sError> {
        info!("deleting secret");
        let dp = DeleteParams::default();
        match self.secrets_api.delete(SECRET_NAME, &dp).await {
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
        base_config: &str,
        prod_config: &str,
    ) -> Result<(), K8sError> {
        info!("patching config map");

        let config_map_json = json!({
          "kind": "ConfigMap",
          "apiVersion": "v1",
          "metadata": {
            "name": CONFIG_MAP_NAME
          },
          "data": {
            "base.yaml": base_config,
            "prod.yaml": prod_config,
          }
        });
        let config_map: ConfigMap = serde_json::from_value(config_map_json)?;

        let pp = PatchParams::apply(CONFIG_MAP_NAME);
        self.config_maps_api
            .patch(CONFIG_MAP_NAME, &pp, &Patch::Apply(config_map))
            .await?;
        info!("patched config map");
        Ok(())
    }

    pub async fn delete_config_map(&self) -> Result<(), K8sError> {
        info!("deleting config map");
        let dp = DeleteParams::default();
        match self.config_maps_api.delete(CONFIG_MAP_NAME, &dp).await {
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

    pub async fn create_or_update_pod(&self) -> Result<(), K8sError> {
        info!("patching pod");

        let pod_json = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": POD_NAME },
            "spec": {
                "volumes": [
                  {
                    "name": "config-file",
                    "configMap": {
                      "name": "replicator-config"
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
                          "name": "bq-service-account-key",
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

        let pp = PatchParams::apply(POD_NAME);
        self.pods_api
            .patch(POD_NAME, &pp, &Patch::Apply(pod))
            .await?;
        info!("patched pod");
        Ok(())
    }

    pub async fn delete_pod(&self) -> Result<(), K8sError> {
        info!("deleting Pod");
        let dp = DeleteParams::default();
        match self.pods_api.delete(POD_NAME, &dp).await {
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
