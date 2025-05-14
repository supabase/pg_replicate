use thiserror::Error;
use tracing::subscriber::{set_global_default, SetGlobalDefaultError};
use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{self, InitError},
};
use tracing_log::{log_tracer::SetLoggerError, LogTracer};
use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    EnvFilter, FmtSubscriber,
};

const DEV_ENV_NAME: &str = "dev";
const PROD_ENV_NAME: &str = "prod";

#[derive(Debug, Error)]
pub enum TracingError {
    #[error("failed to build rolling file appender: {0}")]
    InitAppender(#[from] InitError),

    #[error("failed to init log tracer: {0}")]
    InitLogTracer(#[from] SetLoggerError),

    #[error("failed to set global default subscriber: {0}")]
    SetGlobalDefault(#[from] SetGlobalDefaultError),
}

#[must_use]
pub enum LogFlusher {
    Flusher(WorkerGuard),
    NullFlusher,
}

/// Initializes tracing for the application.
pub fn init_tracing() -> Result<LogFlusher, TracingError> {
    // Initialize the log tracer to capture logs from the `log` crate
    // and send them to the `tracing` subscriber. This captures logs
    // from libraries that use the `log` crate.
    LogTracer::init()?;

    let is_prod =
        std::env::var("APP_ENVIRONMENT").unwrap_or_else(|_| DEV_ENV_NAME.into()) == PROD_ENV_NAME;

    // Set the default log level to `info` if not specified in the RUST_LOG environment variable.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());

    let log_flusher = if is_prod {
        configure_prod_tracing(filter)?
    } else {
        configure_dev_tracing(filter)?
    };

    // Return the log flusher to ensure logs are flushed before the application exits
    // without this the logs in memory may not be flushed to the file
    Ok(log_flusher)
}

fn configure_prod_tracing(filter: EnvFilter) -> Result<LogFlusher, TracingError> {
    let app_name = env!("CARGO_CRATE_NAME");
    let filename_suffix = "log";
    let log_dir = "logs";
    let file_appender = rolling::Builder::new()
        .filename_prefix(app_name)
        .filename_suffix(filename_suffix)
        // rotate the log file every day
        .rotation(rolling::Rotation::DAILY)
        // keep a maximum of 5 log files
        .max_log_files(5)
        .build(log_dir)?;

    // Create a non-blocking appender to avoid blocking the logging thread
    // when writing to the file. This is important for performance.
    let (file_appender, guard) = tracing_appender::non_blocking(file_appender);

    let format = fmt::format()
        .with_level(true)
        // ANSI colors are only for terminal output
        .with_ansi(false)
        // Disable target to reduce noise in the logs
        .with_target(false);

    let subscriber = FmtSubscriber::builder()
        .event_format(format)
        .with_writer(file_appender)
        .json()
        // emit a log event when a span is closed
        // since a request is a span, this will emit a log event
        // when the request is completed
        .with_span_events(FmtSpan::CLOSE)
        .with_env_filter(filter)
        .finish();

    set_global_default(subscriber)?;
    Ok(LogFlusher::Flusher(guard))
}

fn configure_dev_tracing(filter: EnvFilter) -> Result<LogFlusher, TracingError> {
    let format = fmt::format()
        // Emit the log level in the log output
        .with_level(true)
        // Enable ANSI colors for terminal output
        .with_ansi(true)
        // Make it pretty
        .pretty()
        // Disable line number, file, and target in the log output
        // to reduce noise in the logs
        .with_line_number(false)
        .with_file(false)
        .with_target(false);

    let subscriber = FmtSubscriber::builder()
        .event_format(format)
        // emit a log event when a span is closed
        // since a request is a span, this will emit a log event
        // when the request is completed
        .with_span_events(FmtSpan::CLOSE)
        .with_env_filter(filter)
        .finish();

    set_global_default(subscriber)?;
    Ok(LogFlusher::NullFlusher)
}
