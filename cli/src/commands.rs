use rustyline::DefaultEditor;
use thiserror::Error;

use crate::{
    api_client::ApiClient,
    tenants::{create_tenant, read_tenant},
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
            "a" | "ad" | "add" => Command::Add,
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
            "p" | "pi" | "pip" | "pipe" | "pipel" | "pipeli" | "pipelin" | "pipeline"
            | "pipelines" => SubCommand::Pipelines,
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
                println!("error creating tenant: {e:#?}");
            }
        },
        (Command::Add, SubCommand::Sources) => todo!(),
        (Command::Add, SubCommand::Sinks) => todo!(),
        (Command::Add, SubCommand::Pipelines) => todo!(),
        (Command::Update, SubCommand::Tenants) => todo!(),
        (Command::Update, SubCommand::Sources) => todo!(),
        (Command::Update, SubCommand::Sinks) => todo!(),
        (Command::Update, SubCommand::Pipelines) => todo!(),
        (Command::Delete, SubCommand::Tenants) => todo!(),
        (Command::Delete, SubCommand::Sources) => todo!(),
        (Command::Delete, SubCommand::Sinks) => todo!(),
        (Command::Delete, SubCommand::Pipelines) => todo!(),
        (Command::Show, SubCommand::Tenants) => match read_tenant(api_client, editor).await {
            Ok(tenant) => {
                println!("tenant: {tenant}")
            }
            Err(e) => {
                println!("error reading tenant: {e:#?}");
            }
        },
        (Command::Show, SubCommand::Sources) => todo!(),
        (Command::Show, SubCommand::Sinks) => todo!(),
        (Command::Show, SubCommand::Pipelines) => todo!(),
        (Command::List, SubCommand::Tenants) => todo!(),
        (Command::List, SubCommand::Sources) => todo!(),
        (Command::List, SubCommand::Sinks) => todo!(),
        (Command::List, SubCommand::Pipelines) => todo!(),
    }
}
