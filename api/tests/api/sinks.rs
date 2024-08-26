use api::db::sinks::SinkConfig;
use reqwest::StatusCode;

use crate::{
    tenants::create_tenant,
    test_app::{
        spawn_app, CreateSinkRequest, CreateSinkResponse, SinkResponse, TestApp, UpdateSinkRequest,
    },
};

fn new_sink_config() -> SinkConfig {
    SinkConfig::BigQuery {
        project_id: "project-id".to_string(),
        dataset_id: "dataset-id".to_string(),
        service_account_key: "service-account-key".to_string(),
    }
}

fn updated_sink_config() -> SinkConfig {
    SinkConfig::BigQuery {
        project_id: "project-id-updated".to_string(),
        dataset_id: "dataset-id-updated".to_string(),
        service_account_key: "service-account-key-updated".to_string(),
    }
}

pub async fn create_sink_with_config(app: &TestApp, tenant_id: i64, config: SinkConfig) -> i64 {
    let sink = CreateSinkRequest { config };
    let response = app.create_sink(tenant_id, &sink).await;
    let response: CreateSinkResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    response.id
}

pub async fn create_sink(app: &TestApp, tenant_id: i64) -> i64 {
    create_sink_with_config(app, tenant_id, new_sink_config()).await
}

#[tokio::test]
async fn sink_can_be_created() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    // Act
    let sink = CreateSinkRequest {
        config: new_sink_config(),
    };
    let response = app.create_sink(tenant_id, &sink).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateSinkResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, 1);
}

#[tokio::test]
async fn an_existing_sink_can_be_read() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    let sink = CreateSinkRequest {
        config: new_sink_config(),
    };
    let response = app.create_sink(tenant_id, &sink).await;
    let response: CreateSinkResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let sink_id = response.id;

    // Act
    let response = app.read_sink(tenant_id, sink_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: SinkResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, sink_id);
    assert_eq!(response.tenant_id, tenant_id);
    assert_eq!(response.config, sink.config);
}

#[tokio::test]
async fn an_non_existing_sink_cant_be_read() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    // Act
    let response = app.read_sink(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_existing_sink_can_be_updated() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    let sink = CreateSinkRequest {
        config: new_sink_config(),
    };
    let response = app.create_sink(tenant_id, &sink).await;
    let response: CreateSinkResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let sink_id = response.id;

    // Act
    let updated_config = UpdateSinkRequest {
        config: updated_sink_config(),
    };
    let response = app.update_sink(tenant_id, sink_id, &updated_config).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_sink(tenant_id, sink_id).await;
    let response: SinkResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, sink_id);
    assert_eq!(response.tenant_id, tenant_id);
    assert_eq!(response.config, updated_config.config);
}

#[tokio::test]
async fn an_non_existing_sink_cant_be_updated() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    // Act
    let updated_config = UpdateSinkRequest {
        config: updated_sink_config(),
    };
    let response = app.update_sink(tenant_id, 42, &updated_config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_existing_sink_can_be_deleted() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    let sink = CreateSinkRequest {
        config: new_sink_config(),
    };
    let response = app.create_sink(tenant_id, &sink).await;
    let response: CreateSinkResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let sink_id = response.id;

    // Act
    let response = app.delete_sink(tenant_id, sink_id).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_sink(tenant_id, sink_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_non_existing_sink_cant_be_deleted() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    // Act
    let response = app.delete_sink(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn all_sinks_can_be_read() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;
    let sink1_id = create_sink_with_config(&app, tenant_id, new_sink_config()).await;
    let sink2_id = create_sink_with_config(&app, tenant_id, updated_sink_config()).await;

    // Act
    let response = app.read_all_sinks(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: Vec<SinkResponse> = response
        .json()
        .await
        .expect("failed to deserialize response");
    for sink in response {
        if sink.id == sink1_id {
            let config = new_sink_config();
            assert_eq!(sink.tenant_id, tenant_id);
            assert_eq!(sink.config, config);
        } else if sink.id == sink2_id {
            let config = updated_sink_config();
            assert_eq!(sink.tenant_id, tenant_id);
            assert_eq!(sink.config, config);
        }
    }
}
