use api::db::publications::PublicationConfig;
use reqwest::StatusCode;

use crate::{
    sinks::create_sink,
    sources::create_source,
    tenants::create_tenant,
    test_app::{
        spawn_app, CreatePublicationRequest, CreatePublicationResponse, PublicationResponse,
        TestApp, UpdatePublicationRequest,
    },
};

fn new_publication_config() -> PublicationConfig {
    PublicationConfig {
        table_names: vec!["table1".to_string()],
    }
}

fn updated_publication_config() -> PublicationConfig {
    PublicationConfig {
        table_names: vec!["table1".to_string(), "table2".to_string()],
    }
}

pub async fn create_publication(app: &TestApp, tenant_id: i64, source_id: i64) -> i64 {
    create_publication_with_config(
        app,
        tenant_id,
        source_id,
        "new_publication".to_string(),
        new_publication_config(),
    )
    .await
}

pub async fn create_publication_with_config(
    app: &TestApp,
    tenant_id: i64,
    source_id: i64,
    name: String,
    config: PublicationConfig,
) -> i64 {
    let publication = CreatePublicationRequest {
        source_id,
        name,
        config,
    };
    let response = app.create_publication(tenant_id, &publication).await;
    let response: CreatePublicationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    response.id
}

#[tokio::test]
async fn publication_can_be_created() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;

    // Act
    let publication = CreatePublicationRequest {
        source_id,
        name: "new_publication".to_string(),
        config: new_publication_config(),
    };
    let response = app.create_publication(tenant_id, &publication).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreatePublicationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, 1);
}

#[tokio::test]
async fn publication_with_another_tenants_source_cant_be_created() {
    // Arrange
    let app = spawn_app().await;
    let tenant1_id = create_tenant(&app).await;
    let source1_id = create_source(&app, tenant1_id).await;
    let tenant2_id = create_tenant(&app).await;

    // Act
    let publication = CreatePublicationRequest {
        source_id: source1_id,
        name: "new_publication".to_string(),
        config: new_publication_config(),
    };
    let response = app.create_publication(tenant2_id, &publication).await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn an_existing_publication_can_be_read() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let sink_id = create_sink(&app, tenant_id).await;
    let publication = CreatePublicationRequest {
        source_id,
        name: "new_publication".to_string(),
        config: new_publication_config(),
    };
    let response = app.create_publication(tenant_id, &publication).await;
    let response: CreatePublicationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let publication_id = response.id;

    // Act
    let response = app.read_publication(tenant_id, publication_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: PublicationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, sink_id);
    assert_eq!(response.tenant_id, tenant_id);
    assert_eq!(response.source_id, source_id);
    assert_eq!(response.name, "new_publication".to_string());
    assert_eq!(response.config, publication.config);
}

#[tokio::test]
async fn an_non_existing_publication_cant_be_read() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    // Act
    let response = app.read_publication(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_existing_publication_can_be_updated() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;

    let publication = CreatePublicationRequest {
        source_id,
        name: "new_publication".to_string(),
        config: new_publication_config(),
    };
    let response = app.create_publication(tenant_id, &publication).await;
    let response: CreatePublicationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let publication_id = response.id;

    // Act
    let updated_source_id = create_source(&app, tenant_id).await;
    let updated_config = UpdatePublicationRequest {
        source_id: updated_source_id,
        name: "updated_publication".to_string(),
        config: updated_publication_config(),
    };
    let response = app
        .update_publication(tenant_id, publication_id, &updated_config)
        .await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_publication(tenant_id, publication_id).await;
    let response: PublicationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, publication_id);
    assert_eq!(response.tenant_id, tenant_id);
    assert_eq!(response.source_id, updated_source_id);
    assert_eq!(response.name, "updated_publication".to_string());
    assert_eq!(response.config, updated_config.config);
}

#[tokio::test]
async fn publication_with_another_tenants_source_cant_be_updated() {
    // Arrange
    let app = spawn_app().await;
    let tenant1_id = create_tenant(&app).await;
    let source1_id = create_source(&app, tenant1_id).await;
    let tenant2_id = create_tenant(&app).await;
    let source2_id = create_source(&app, tenant2_id).await;

    // Act
    let publication = CreatePublicationRequest {
        source_id: source1_id,
        name: "new_publication".to_string(),
        config: new_publication_config(),
    };
    let response = app.create_publication(tenant1_id, &publication).await;
    let response: CreatePublicationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let publication_id = response.id;
    let updated_config = UpdatePublicationRequest {
        source_id: source2_id,
        name: "updated_publication".to_string(),
        config: updated_publication_config(),
    };
    let response = app
        .update_publication(tenant1_id, publication_id, &updated_config)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn an_non_existing_publication_cant_be_updated() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;

    // Act
    let updated_config = UpdatePublicationRequest {
        source_id,
        name: "updated_publication".to_string(),
        config: updated_publication_config(),
    };
    let response = app.update_publication(tenant_id, 42, &updated_config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_existing_publication_can_be_deleted() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;

    let publication = CreatePublicationRequest {
        source_id,
        name: "new_publication".to_string(),
        config: new_publication_config(),
    };
    let response = app.create_publication(tenant_id, &publication).await;
    let response: CreatePublicationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let publication_id = response.id;

    // Act
    let response = app.delete_publication(tenant_id, publication_id).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_publication(tenant_id, publication_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_non_existing_publication_cant_be_deleted() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    // Act
    let response = app.delete_publication(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn all_publications_can_be_read() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let publication1_id = create_publication_with_config(
        &app,
        tenant_id,
        source_id,
        "new_publication".to_string(),
        new_publication_config(),
    )
    .await;
    let publication2_id = create_publication_with_config(
        &app,
        tenant_id,
        source_id,
        "updated_publication".to_string(),
        updated_publication_config(),
    )
    .await;

    // Act
    let response = app.read_all_publications(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: Vec<PublicationResponse> = response
        .json()
        .await
        .expect("failed to deserialize response");
    for publication in response {
        if publication.id == publication1_id {
            let config = new_publication_config();
            assert_eq!(publication.tenant_id, tenant_id);
            assert_eq!(publication.name, "new_publication".to_string());
            assert_eq!(publication.config, config);
        } else if publication.id == publication2_id {
            let config = updated_publication_config();
            assert_eq!(publication.tenant_id, tenant_id);
            assert_eq!(publication.name, "updated_publication".to_string());
            assert_eq!(publication.config, config);
        }
    }
}
