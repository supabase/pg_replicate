use k8s_openapi::api::core::v1::{ConfigMap, Pod, Secret};
use serde_json::json;
use thiserror::Error;
use tracing::*;

use kube::{
    api::{Api, Patch, PatchParams, ResourceExt},
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

    pub async fn create_or_update_bq_service_account_key_secret(
        &self,
        bq_service_account_key: &str,
    ) -> Result<(), K8sError> {
        info!("creating BQ service account key secret");

        let secret_name = "bq-service-account-key";
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

        let pp = PatchParams::apply(secret_name);
        match self
            .secrets_api
            .patch(secret_name, &pp, &Patch::Apply(secret))
            .await
        {
            Ok(o) => {
                info!("patched Secret {}", o.name_any());
            }
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }

    pub async fn create_or_update_config_map(
        &self,
        base_config: &str,
        prod_config: &str,
    ) -> Result<(), K8sError> {
        info!("creating config map");

        let config_map_name = "replicator-config";
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

        let pp = PatchParams::apply(config_map_name);
        match self
            .config_maps_api
            .patch(config_map_name, &pp, &Patch::Apply(config_map))
            .await
        {
            Ok(cm) => {
                info!("patched ConfigMap {}", cm.name_any());
            }
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    pub async fn create_or_update_pod(&self) -> Result<(), K8sError> {
        info!("creating Pod instance replicator");

        let pod_name = "replicator";
        let pod_json = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": pod_name },
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

        let pp = PatchParams::apply(pod_name);
        match self.pods_api.patch(pod_name, &pp, &Patch::Apply(pod)).await {
            Ok(p) => {
                info!("patched Pod {}", p.name_any());
            }
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }
}
