use crate::helpers::{spawn_app, TenantRequest, TenantResponse};

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
    let _response: TenantResponse = response.json().await.unwrap();
}
