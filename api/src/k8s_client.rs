use k8s_openapi::api::core::v1::{ConfigMap, Pod, Secret};
use serde_json::json;
use thiserror::Error;
use tracing::*;

use kube::{
    api::{Api, PostParams, ResourceExt},
    Client,
};

#[derive(Debug, Error)]
pub enum K8sError {
    #[error["serde_json error: {0}"]]
    Serde(#[from] serde_json::error::Error),

    #[error["kube error: {0}"]]
    Kube(#[from] kube::Error),
}

pub async fn create_bq_service_account_key_secret(
    bq_service_account_key: &str,
) -> Result<(), K8sError> {
    info!("creating BQ service account key secret");

    let secret: Secret = serde_json::from_value(json!({
      "apiVersion": "v1",
      "kind": "Secret",
      "metadata": {
        "name": "bq-service-account-key"
      },
      "type": "Opaque",
      "stringData": {
        "service-account-key": bq_service_account_key,
      }
    }))?;

    let client = Client::try_default().await?;
    let secrets: Api<Secret> = Api::default_namespaced(client);

    let pp = PostParams::default();
    match secrets.create(&pp, &secret).await {
        Ok(o) => {
            let name = o.name_any();
            assert_eq!(secret.name_any(), name);
            info!("created Secret {}", name);
        }
        Err(kube::Error::Api(ae)) => {
            error!("Error: {ae}");
            assert_eq!(ae.code, 409);
        } // if you skipped delete, for instance
        Err(e) => return Err(e.into()), // any other case is probably bad
    }

    Ok(())
}

pub async fn create_config_map(base_config: &str, prod_config: &str) -> Result<(), K8sError> {
    info!("creating config map");

    let cm: ConfigMap = serde_json::from_value(json!({
      "kind": "ConfigMap",
      "apiVersion": "v1",
      "metadata": {
        "name": "replicator-config"
      },
      "data": {
        "base.yaml": base_config,
        "prod.yaml": prod_config,
      }
    }))?;

    let client = Client::try_default().await?;
    let config_maps: Api<ConfigMap> = Api::default_namespaced(client);

    let pp = PostParams::default();
    match config_maps.create(&pp, &cm).await {
        Ok(o) => {
            let name = o.name_any();
            assert_eq!(cm.name_any(), name);
            info!("created ConfigMap {}", name);
        }
        Err(kube::Error::Api(ae)) => {
            error!("Error: {ae}");
            assert_eq!(ae.code, 409);
        } // if you skipped delete, for instance
        Err(e) => return Err(e.into()), // any other case is probably bad
    }
    Ok(())
}

pub async fn create_pod() -> Result<(), K8sError> {
    info!("creating Pod instance replicator");

    let client = Client::try_default().await?;
    let pods: Api<Pod> = Api::default_namespaced(client);

    let p: Pod = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "replicator" },
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
    }))?;

    let pp = PostParams::default();
    match pods.create(&pp, &p).await {
        Ok(o) => {
            let name = o.name_any();
            assert_eq!(p.name_any(), name);
            info!("created Pod {}", name);
        }
        Err(kube::Error::Api(ae)) => {
            error!("Error: {ae}");
            assert_eq!(ae.code, 409);
        } // if you skipped delete, for instance
        Err(e) => return Err(e.into()), // any other case is probably bad
    }
    Ok(())
}
