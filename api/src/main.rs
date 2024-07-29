use std::net::TcpListener;

use api::run;

#[actix_web::main]
pub async fn main() -> Result<(), std::io::Error> {
    let listener = TcpListener::bind("127.0.0.1:8000")?;
    run(listener)?.await
}
