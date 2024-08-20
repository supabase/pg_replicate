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
    let response = app.create_tenant(&tenant).await;

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
    let response = app.create_tenant(&tenant).await;
    let response: TenantIdResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let tenant_id = response.id;

    // Act
    let response = app.read_tenant(tenant_id).await;

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
    let response = app.read_tenant(42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_existing_tenant_can_be_updated() {
    // Arrange
    let app = spawn_app().await;
    let tenant = TenantRequest {
        name: "NewTenant".to_string(),
    };
    let response = app.create_tenant(&tenant).await;
    let response: TenantIdResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let tenant_id = response.id;

    // Act
    let updated_tenant = TenantRequest {
        name: "UpdatedTenant".to_string(),
    };
    let response = app.update_tenant(tenant_id, &updated_tenant).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_tenant(tenant_id).await;
    let response: TenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, tenant_id);
    assert_eq!(response.name, updated_tenant.name);
}

#[tokio::test]
async fn an_non_existing_tenant_cant_be_updated() {
    // Arrange
    let app = spawn_app().await;

    // Act
    let updated_tenant = TenantRequest {
        name: "UpdatedTenant".to_string(),
    };
    let response = app.update_tenant(42, &updated_tenant).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_existing_tenant_can_be_deleted() {
    // Arrange
    let app = spawn_app().await;
    let tenant = TenantRequest {
        name: "NewTenant".to_string(),
    };
    let response = app.create_tenant(&tenant).await;
    let response: TenantIdResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let tenant_id = response.id;

    // Act
    let response = app.delete_tenant(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_tenant(tenant_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_non_existing_tenant_cant_be_deleted() {
    // Arrange
    let app = spawn_app().await;

    // Act
    let response = app.delete_tenant(42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
