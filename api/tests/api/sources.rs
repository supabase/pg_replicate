use api::db::sources::SourceConfig;

use crate::test_app::{
    spawn_app, CreateSourceRequest, CreateSourceResponse, CreateTenantRequest, CreateTenantResponse,
};

fn new_source_config() -> SourceConfig {
    SourceConfig::Postgres {
        host: "localhost".to_string(),
        port: 5432,
        name: "postgres".to_string(),
        username: "postgres".to_string(),
        password: Some("postgres".to_string()),
        slot_name: "slot".to_string(),
        publication: "publication".to_string(),
    }
}

#[tokio::test]
async fn tenant_can_be_created_with_supabase_project_ref() {
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
    let source = CreateSourceRequest {
        tenant_id,
        config: new_source_config(),
    };
    let response = app.create_source(&source).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, 1);
}
