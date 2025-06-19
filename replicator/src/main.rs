use telemetry::init_tracing;

mod config;
mod environment;
mod core;

// APP_SOURCE__POSTGRES__PASSWORD and APP_DESTINATION__BIG_QUERY__PROJECT_ID environment variables must be set
// before running because these are sensitive values which can't be configured in the config files
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app_name = env!("CARGO_BIN_NAME");
    
    // We pass `emit_on_span_close = false` to avoid emitting logs on span close
    // for replicator because it is not a web server, and we don't need to emit logs
    // for every closing span.
    let _log_flusher = init_tracing(app_name, false)?;
    
    Ok(())
}