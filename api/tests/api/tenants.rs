use api::utils::generate_random_alpha_str;
use reqwest::StatusCode;

use crate::test_app::{
    spawn_app, CreateTenantRequest, CreateTenantResponse, TenantResponse, UpdateTenantRequest,
};

#[tokio::test]
async fn tenant_can_be_created_with_supabase_project_ref() {
    // Arrange
    let app = spawn_app().await;

    // Act
    let supabase_project_ref = Some(generate_random_alpha_str(20));
    let tenant = CreateTenantRequest {
        name: "NewTenant".to_string(),
        supabase_project_ref,
    };
    let response = app.create_tenant(&tenant).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, 1);

    let tenant_id = response.id;
    let response = app.read_tenant(tenant_id).await;
    let response: TenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");

    assert_eq!(response.id, tenant_id);
    assert_eq!(response.name, tenant.name);
    assert_eq!(response.supabase_project_ref, tenant.supabase_project_ref);
    assert_eq!(response.prefix, tenant.supabase_project_ref.unwrap());
}

#[tokio::test]
async fn tenant_can_be_created_without_supabase_project_ref() {
    // Arrange
    let app = spawn_app().await;

    // Act
    let tenant = CreateTenantRequest {
        name: "NewTenant".to_string(),
        supabase_project_ref: None,
    };
    let response = app.create_tenant(&tenant).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, 1);

    let tenant_id = response.id;
    let response = app.read_tenant(tenant_id).await;
    let response: TenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");

    assert_eq!(response.id, tenant_id);
    assert_eq!(response.name, tenant.name);
    assert_eq!(response.supabase_project_ref, None);
    assert!(response.prefix.len() == 20);
}

#[tokio::test]
async fn an_existing_tenant_can_be_read() {
    // Arrange
    let app = spawn_app().await;
    let tenant = CreateTenantRequest {
        name: "NewTenant".to_string(),
        supabase_project_ref: None,
    };
    let response = app.create_tenant(&tenant).await;
    let response: CreateTenantResponse = response
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
    let tenant = CreateTenantRequest {
        name: "NewTenant".to_string(),
        supabase_project_ref: None,
    };
    let response = app.create_tenant(&tenant).await;
    let response: CreateTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let tenant_id = response.id;

    // Act
    let updated_tenant = UpdateTenantRequest {
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
    let updated_tenant = UpdateTenantRequest {
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
    let tenant = CreateTenantRequest {
        name: "NewTenant".to_string(),
        supabase_project_ref: None,
    };
    let response = app.create_tenant(&tenant).await;
    let response: CreateTenantResponse = response
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
