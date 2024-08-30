use rustyline::DefaultEditor;
use thiserror::Error;

use crate::{
    api_client::ApiClient,
    pipelines::{create_pipeline, delete_pipeline, list_pipelines, show_pipeline, update_pipeline},
    sinks::{create_sink, delete_sink, list_sinks, show_sink, update_sink},
    sources::{create_source, delete_source, list_sources, show_source, update_source},
    tenants::{create_tenant, delete_tenant, list_tenants, show_tenant, update_tenant},
};

pub enum Command {
    Add,
    Update,
    Delete,
    Show,
    List,
}

#[derive(Debug, Error)]
pub enum CommandParseError {
    #[error("invalid command: {0}")]
    InvalidCommand(String),
}

impl TryFrom<&str> for Command {
    type Error = CommandParseError;

    fn try_from(command: &str) -> Result<Self, Self::Error> {
        Ok(match command {
            "a" | "ad" | "add" | "c" | "cr" | "cre" | "crea" | "creat" | "create" => Command::Add,
            "u" | "up" | "upd" | "upda" | "updat" | "update" => Command::Update,
            "d" | "de" | "del" | "dele" | "delet" | "delete" => Command::Delete,
            "s" | "sh" | "sho" | "show" => Command::Show,
            "l" | "li" | "lis" | "list" => Command::List,
            command => return Err(CommandParseError::InvalidCommand(command.to_string())),
        })
    }
}

#[derive(Debug, Error)]
pub enum SubCommandParseError {
    #[error("invalid subcommand: {0}")]
    InvalidSubCommand(String),
}

pub enum SubCommand {
    Tenants,
    Sources,
    Sinks,
    Pipelines,
}

impl TryFrom<&str> for SubCommand {
    type Error = SubCommandParseError;

    fn try_from(subcommand: &str) -> Result<Self, Self::Error> {
        Ok(match subcommand {
            "t" | "te" | "ten" | "tena" | "tenan" | "tenant" | "tenants" => SubCommand::Tenants,
            "so" | "sou" | "sour" | "sourc" | "source" | "sources" => SubCommand::Sources,
            "si" | "sin" | "sink" | "sinks" => SubCommand::Sinks,
            "pi" | "pip" | "pipe" | "pipel" | "pipeli" | "pipelin" | "pipeline" | "pipelines" => {
                SubCommand::Pipelines
            }
            subcommand => {
                return Err(SubCommandParseError::InvalidSubCommand(
                    subcommand.to_string(),
                ))
            }
        })
    }
}

pub async fn execute_commands(
    command: Command,
    subcommand: SubCommand,
    api_client: &ApiClient,
    editor: &mut DefaultEditor,
) {
    match (command, subcommand) {
        (Command::Add, SubCommand::Tenants) => match create_tenant(api_client, editor).await {
            Ok(tenant) => {
                println!("tenant created: {tenant}");
            }
            Err(e) => {
                println!("error creating tenant: {e:?}");
            }
        },
        (Command::Add, SubCommand::Sources) => match create_source(api_client, editor).await {
            Ok(source) => {
                println!("source created: {source}");
            }
            Err(e) => {
                println!("error creating source: {e:?}");
            }
        },
        (Command::Add, SubCommand::Sinks) => match create_sink(api_client, editor).await {
            Ok(sink) => {
                println!("sink created: {sink}");
            }
            Err(e) => {
                println!("error creating sink: {e:?}");
            }
        },
        (Command::Add, SubCommand::Pipelines) => match create_pipeline(api_client, editor).await {
            Ok(pipeline) => {
                println!("pipeline created: {pipeline}");
            }
            Err(e) => {
                println!("error creating pipeline: {e:?}");
            }
        },
        (Command::Update, SubCommand::Tenants) => match update_tenant(api_client, editor).await {
            Ok(()) => println!("tenant updated"),
            Err(e) => {
                println!("error updating tenant: {e:?}");
            }
        },
        (Command::Update, SubCommand::Sources) => match update_source(api_client, editor).await {
            Ok(()) => println!("source updated"),
            Err(e) => {
                println!("error updating source: {e:?}");
            }
        },
        (Command::Update, SubCommand::Sinks) => match update_sink(api_client, editor).await {
            Ok(()) => println!("sink updated"),
            Err(e) => {
                println!("error updating sink: {e:?}");
            }
        },
        (Command::Update, SubCommand::Pipelines) => match update_pipeline(api_client, editor).await
        {
            Ok(()) => println!("pipeline updated"),
            Err(e) => {
                println!("error updating pipeline: {e:?}");
            }
        },
        (Command::Delete, SubCommand::Tenants) => match delete_tenant(api_client, editor).await {
            Ok(()) => println!("tenant deleted"),
            Err(e) => {
                println!("error deleting tenant: {e:?}");
            }
        },
        (Command::Delete, SubCommand::Sources) => match delete_source(api_client, editor).await {
            Ok(()) => println!("source deleted"),
            Err(e) => {
                println!("error deleting source: {e:?}");
            }
        },
        (Command::Delete, SubCommand::Sinks) => match delete_sink(api_client, editor).await {
            Ok(()) => println!("sink deleted"),
            Err(e) => {
                println!("error deleting sink: {e:?}");
            }
        },
        (Command::Delete, SubCommand::Pipelines) => match delete_pipeline(api_client, editor).await
        {
            Ok(()) => println!("pipeline deleted"),
            Err(e) => {
                println!("error deleting pipeline: {e:?}");
            }
        },
        (Command::Show, SubCommand::Tenants) => match show_tenant(api_client, editor).await {
            Ok(tenant) => {
                println!("tenant: {tenant}")
            }
            Err(e) => {
                println!("error reading tenant: {e:?}");
            }
        },
        (Command::Show, SubCommand::Sources) => match show_source(api_client, editor).await {
            Ok(source) => {
                println!("source: {source}")
            }
            Err(e) => {
                println!("error reading source: {e:?}");
            }
        },
        (Command::Show, SubCommand::Sinks) => match show_sink(api_client, editor).await {
            Ok(sink) => {
                println!("sink: {sink}")
            }
            Err(e) => {
                println!("error reading sink: {e:?}");
            }
        },
        (Command::Show, SubCommand::Pipelines) => match show_pipeline(api_client, editor).await {
            Ok(pipeline) => {
                println!("pipeline: {pipeline}")
            }
            Err(e) => {
                println!("error reading pipeline: {e:?}");
            }
        },
        (Command::List, SubCommand::Tenants) => match list_tenants(api_client).await {
            Ok(tenants) => {
                println!("tenants: ");
                for tenant in tenants {
                    println!("  {tenant}")
                }
            }
            Err(e) => {
                println!("error reading tenant: {e:?}");
            }
        },
        (Command::List, SubCommand::Sources) => match list_sources(api_client, editor).await {
            Ok(sources) => {
                println!("sources: ");
                for source in sources {
                    println!("  {source}")
                }
            }
            Err(e) => {
                println!("error reading source: {e:?}");
            }
        },
        (Command::List, SubCommand::Sinks) => match list_sinks(api_client, editor).await {
            Ok(sinks) => {
                println!("sinks: ");
                for sink in sinks {
                    println!("  {sink}")
                }
            }
            Err(e) => {
                println!("error reading sink: {e:?}");
            }
        },
        (Command::List, SubCommand::Pipelines) => match list_pipelines(api_client, editor).await {
            Ok(pipelines) => {
                println!("pipelines: ");
                for pipeline in pipelines {
                    println!("  {pipeline}")
                }
            }
            Err(e) => {
                println!("error reading pipelines: {e:?}");
            }
        },
    }
}
