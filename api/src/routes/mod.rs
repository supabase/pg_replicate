use serde::Serialize;

pub mod health_check;
pub mod pipelines;
pub mod sinks;
pub mod sources;
pub mod tenants;

#[derive(Serialize)]
pub struct ErrorMessage {
    pub error: String,
}
