use std::error::Error;

use actix_web::{get, App, HttpResponse, HttpServer, Responder};

mod queue;

#[actix_web::main]
async fn main() -> Result<(), Box<dyn Error>> {
    HttpServer::new(|| App::new().service(health_check))
        .bind("127.0.0.1:8000")?
        .run()
        .await?;

    Ok(())
}

#[get("/health_check")]
async fn health_check() -> impl Responder {
    HttpResponse::Ok().body("ok")
}
