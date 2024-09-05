use std::fmt::{Debug, Display};

use api::{
    configuration::get_configuration,
    k8s_client::HttpK8sClient,
    startup::Application,
    telemetry::{get_subscriber, init_subscriber},
    worker::run_worker_until_stopped,
};
use tokio::task::JoinError;
use tracing::info;

#[actix_web::main]
pub async fn main() -> anyhow::Result<()> {
    let subscriber = get_subscriber("api".into(), "info".into(), std::io::stdout);
    init_subscriber(subscriber);
    let configuration = get_configuration().expect("Failed to read configuration.");
    info!("{configuration}");
    let application = Application::build(configuration.clone()).await?;
    let application_task = tokio::spawn(application.run_until_stopped());

    let k8s_client = HttpK8sClient::new().await?;
    let worker_task = tokio::spawn(run_worker_until_stopped(configuration, k8s_client));

    tokio::select! {
        o = application_task => report_exit("API", o),
        o = worker_task =>  report_exit("Background worker", o),
    };

    Ok(())
}

fn report_exit(task_name: &str, outcome: Result<Result<(), impl Debug + Display>, JoinError>) {
    match outcome {
        Ok(Ok(())) => {
            tracing::info!("{} has exited", task_name)
        }
        Ok(Err(e)) => {
            tracing::error!(
                error.cause_chain = ?e,
                error.message = %e,
                "{} failed",
                task_name
            )
        }
        Err(e) => {
            tracing::error!(
                error.cause_chain = ?e,
                error.message = %e,
                "{}' task failed to complete",
                task_name
            )
        }
    }
}
