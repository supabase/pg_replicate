use reqwest::StatusCode;

use crate::helpers::{spawn_app, TenantIdResponse, TenantRequest, TenantResponse};

#[tokio::test]
async fn tenant_can_be_saved() {
    // Arrange
    let app = spawn_app().await;

    // Act
    let tenant = TenantRequest {
        name: "NewTenant".to_string(),
    };
    let response = app.post_tenant(&tenant).await;

    // Assert
    assert!(response.status().is_success());
    let response: TenantIdResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, 1);
}

#[tokio::test]
async fn an_existing_tenant_can_be_read() {
    // Arrange
    let app = spawn_app().await;
    let tenant = TenantRequest {
        name: "NewTenant".to_string(),
    };
    let response = app.post_tenant(&tenant).await;
    let response: TenantIdResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let tenant_id = response.id;

    // Act
    let response = app.get_tenant(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: TenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, tenant_id);
    assert_eq!(response.name, tenant.name);
}

#[tokio::test]
async fn an_non_existing_tenant_cant_be_read() {
    // Arrange
    let app = spawn_app().await;

    // Act
    let response = app.get_tenant(42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
