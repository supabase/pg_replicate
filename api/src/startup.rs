use std::{net::TcpListener, sync::Arc};

use actix_web::{dev::Server, web, App, HttpServer};
use actix_web_httpauth::middleware::HttpAuthentication;
use aws_lc_rs::aead::{RandomizedNonceKey, AES_256_GCM};
use base64::{prelude::BASE64_STANDARD, Engine};
use postgres::sqlx::config::PgConnectionConfig;
use sqlx::{postgres::PgPoolOptions, PgPool};
use tracing_actix_web::TracingLogger;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    authentication::auth_validator,
    config::ApiConfig,
    db::publications::Publication,
    encryption,
    k8s_client::HttpK8sClient,
    routes::{
        destinations::{
            create_destination, delete_destination, read_all_destinations, read_destination,
            update_destination, GetDestinationResponse, PostDestinationRequest,
            PostDestinationResponse,
        },
        destinations_pipelines::{
            create_destinations_and_pipelines, update_destinations_and_pipelines,
            PostDestinationPipelineRequest, PostDestinationPipelineResponse,
        },
        health_check::health_check,
        images::{
            create_image, delete_image, read_all_images, read_image, update_image,
            GetImageResponse, PostImageRequest, PostImageResponse,
        },
        pipelines::{
            create_pipeline, delete_pipeline, get_pipeline_status, read_all_pipelines,
            read_pipeline, start_pipeline, stop_all_pipelines, stop_pipeline, update_pipeline,
            GetPipelineResponse, PostPipelineRequest, PostPipelineResponse,
        },
        sources::{
            create_source, delete_source,
            publications::{
                create_publication, delete_publication, read_all_publications, read_publication,
                update_publication, CreatePublicationRequest, UpdatePublicationRequest,
            },
            read_all_sources, read_source,
            tables::read_table_names,
            update_source, GetSourceResponse, PostSourceRequest, PostSourceResponse,
        },
        tenants::{
            create_or_update_tenant, create_tenant, delete_tenant, read_all_tenants, read_tenant,
            update_tenant, CreateTenantRequest, GetTenantResponse, PostTenantResponse,
        },
        tenants_sources::{
            create_tenant_and_source, CreateTenantSourceRequest, PostTenantSourceResponse,
        },
    },
    span_builder::ApiRootSpanBuilder,
};

pub struct Application {
    port: u16,
    server: Server,
}

impl Application {
    pub async fn build(configuration: ApiConfig) -> Result<Self, anyhow::Error> {
        let connection_pool = get_connection_pool(&configuration.database);

        let address = format!(
            "{}:{}",
            configuration.application.host, configuration.application.port
        );
        let listener = TcpListener::bind(address)?;
        let port = listener.local_addr()?.port();
        let key_bytes = BASE64_STANDARD.decode(&configuration.encryption_key.key)?;
        let key = RandomizedNonceKey::new(&AES_256_GCM, &key_bytes)?;
        let encryption_key = encryption::EncryptionKey {
            id: configuration.encryption_key.id,
            key,
        };
        let api_key = configuration.api_key;
        let k8s_client = HttpK8sClient::new().await?;
        let server = run(
            listener,
            connection_pool,
            encryption_key,
            api_key,
            Some(k8s_client),
        )
        .await?;

        Ok(Self { port, server })
    }

    pub async fn migrate_database(config: PgConnectionConfig) -> Result<(), anyhow::Error> {
        let connection_pool = get_connection_pool(&config);

        sqlx::migrate!("./migrations").run(&connection_pool).await?;

        Ok(())
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn run_until_stopped(self) -> Result<(), std::io::Error> {
        self.server.await
    }
}

pub fn get_connection_pool(config: &PgConnectionConfig) -> PgPool {
    PgPoolOptions::new().connect_lazy_with(config.with_db())
}

// HttpK8sClient is wrapped in an option because creating it
// in tests involves setting a default CryptoProvider and it
// interferes with parallel tasks because only one can be set.
pub async fn run(
    listener: TcpListener,
    connection_pool: PgPool,
    encryption_key: encryption::EncryptionKey,
    api_key: String,
    http_k8s_client: Option<HttpK8sClient>,
) -> Result<Server, anyhow::Error> {
    let connection_pool = web::Data::new(connection_pool);
    let encryption_key = web::Data::new(encryption_key);
    let api_key = web::Data::new(api_key);
    let k8s_client = http_k8s_client.map(|client| web::Data::new(Arc::new(client)));

    #[derive(OpenApi)]
    #[openapi(
        paths(
            crate::routes::health_check::health_check,
            crate::routes::images::create_image,
            crate::routes::images::read_image,
            crate::routes::images::update_image,
            crate::routes::images::delete_image,
            crate::routes::images::read_all_images,
            crate::routes::pipelines::create_pipeline,
            crate::routes::pipelines::read_pipeline,
            crate::routes::pipelines::update_pipeline,
            crate::routes::pipelines::delete_pipeline,
            crate::routes::pipelines::read_all_pipelines,
            crate::routes::pipelines::get_pipeline_status,
            crate::routes::tenants::create_tenant,
            crate::routes::tenants::create_or_update_tenant,
            crate::routes::tenants::read_tenant,
            crate::routes::tenants::update_tenant,
            crate::routes::tenants::delete_tenant,
            crate::routes::tenants::read_all_tenants,
            crate::routes::sources::create_source,
            crate::routes::sources::read_source,
            crate::routes::sources::update_source,
            crate::routes::sources::delete_source,
            crate::routes::sources::read_all_sources,
            crate::routes::sources::publications::create_publication,
            crate::routes::sources::publications::read_publication,
            crate::routes::sources::publications::update_publication,
            crate::routes::sources::publications::delete_publication,
            crate::routes::sources::publications::read_all_publications,
            crate::routes::sources::tables::read_table_names,
            crate::routes::destinations::create_destination,
            crate::routes::destinations::read_destination,
            crate::routes::destinations::update_destination,
            crate::routes::destinations::delete_destination,
            crate::routes::destinations::read_all_destinations,
            crate::routes::tenants_sources::create_tenant_and_source,
            crate::routes::destinations_pipelines::create_destinations_and_pipelines,
            crate::routes::destinations_pipelines::update_destinations_and_pipelines,
        ),
        components(schemas(
            PostImageRequest,
            PostImageResponse,
            GetImageResponse,
            PostPipelineRequest,
            PostPipelineResponse,
            GetPipelineResponse,
            CreateTenantRequest,
            PostTenantResponse,
            GetTenantResponse,
            PostSourceRequest,
            PostSourceResponse,
            GetSourceResponse,
            CreatePublicationRequest,
            UpdatePublicationRequest,
            Publication,
            PostDestinationRequest,
            PostDestinationResponse,
            GetDestinationResponse,
            CreateTenantSourceRequest,
            PostTenantSourceResponse,
            PostDestinationPipelineRequest,
            PostDestinationPipelineResponse,
        ))
    )]
    struct ApiDoc;

    //TODO: replace all the context_path = v1 in route modules with the nest attribute
    //when it is available in utoipa 5.0.0: https://github.com/juhaku/utoipa/pull/930
    let openapi = ApiDoc::openapi();

    let server = HttpServer::new(move || {
        let tracing_middleware = TracingLogger::<ApiRootSpanBuilder>::new();
        let authentication = HttpAuthentication::bearer(auth_validator);
        let app = App::new()
            .wrap(tracing_middleware)
            .service(health_check)
            .service(
                SwaggerUi::new("/swagger-ui/{_:.*}").url("/api-docs/openapi.json", openapi.clone()),
            )
            .service(
                web::scope("v1")
                    .wrap(authentication)
                    //tenants
                    .service(create_tenant)
                    .service(create_or_update_tenant)
                    .service(read_tenant)
                    .service(update_tenant)
                    .service(delete_tenant)
                    .service(read_all_tenants)
                    //sources
                    .service(create_source)
                    .service(read_source)
                    .service(update_source)
                    .service(delete_source)
                    .service(read_all_sources)
                    //destinations
                    .service(create_destination)
                    .service(read_destination)
                    .service(update_destination)
                    .service(delete_destination)
                    .service(read_all_destinations)
                    //pipelines
                    .service(create_pipeline)
                    .service(read_pipeline)
                    .service(update_pipeline)
                    .service(delete_pipeline)
                    .service(read_all_pipelines)
                    .service(start_pipeline)
                    .service(stop_pipeline)
                    .service(stop_all_pipelines)
                    .service(get_pipeline_status)
                    //tables
                    .service(read_table_names)
                    //publications
                    .service(create_publication)
                    .service(read_publication)
                    .service(update_publication)
                    .service(delete_publication)
                    .service(read_all_publications)
                    //images
                    .service(create_image)
                    .service(read_image)
                    .service(update_image)
                    .service(delete_image)
                    .service(read_all_images)
                    //tenants_sources
                    .service(create_tenant_and_source)
                    // destinations-pipelines
                    .service(create_destinations_and_pipelines)
                    .service(update_destinations_and_pipelines),
            )
            .app_data(connection_pool.clone())
            .app_data(encryption_key.clone())
            .app_data(api_key.clone());
        if let Some(k8s_client) = k8s_client.clone() {
            app.app_data(k8s_client.clone())
        } else {
            app
        }
    })
    .listen(listener)?
    .run();

    Ok(server)
}
