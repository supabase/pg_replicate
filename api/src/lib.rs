use std::net::TcpListener;

use actix_web::{dev::Server, get, web, App, HttpResponse, HttpServer, Responder};
use configuration::DatabaseSettings;
use sqlx::{postgres::PgPoolOptions, PgPool};

pub mod configuration;
mod queue;

pub fn get_connection_pool(configuration: &DatabaseSettings) -> PgPool {
    PgPoolOptions::new().connect_lazy_with(configuration.with_db())
}

pub fn run(listener: TcpListener, connection_pool: PgPool) -> Result<Server, std::io::Error> {
    let connection_pool = web::Data::new(connection_pool);
    let server = HttpServer::new(move || {
        App::new()
            .service(health_check)
            .app_data(connection_pool.clone())
    })
    .listen(listener)?
    .run();

    Ok(server)
}

#[get("/health_check")]
async fn health_check() -> impl Responder {
    HttpResponse::Ok().body("ok")
}
