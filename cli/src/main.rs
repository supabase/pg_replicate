use std::num::ParseIntError;

use api_client::{ApiClient, ApiClientError};
use commands::{execute_commands, Command, SubCommand};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use thiserror::Error;

mod api_client;
mod commands;
mod pipelines;
mod sinks;
mod sources;
mod tenants;

#[derive(Debug, Error)]
pub enum CliError {
    #[error("readline error: {0}")]
    Readline(#[from] ReadlineError),

    #[error("api client error: {0}")]
    ApiClient(#[from] ApiClientError),

    #[error("parse int error: {0}")]
    ParseInt(#[from] ParseIntError),
}

#[tokio::main]
async fn main() -> rustyline::Result<()> {
    let address = "http://127.0.0.1:8000".to_string();
    let api_client = ApiClient::new(address);
    let mut editor = DefaultEditor::new()?;
    println!("replicator api CLI version 0.1.0");
    println!("type help or ? for help");
    println!();
    loop {
        let readline = editor.readline("> ");
        match readline {
            Ok(line) => match line.to_lowercase().as_str() {
                "quit" | "exit" => {
                    break;
                }
                "help" | "?" => {
                    print_help();
                }
                command => {
                    let command = command.trim();
                    let tokens: Vec<&str> = command.split_whitespace().collect();
                    if tokens.len() != 2 {
                        print_invalid_command_help(command);
                        continue;
                    }
                    let main_command = tokens[0].trim().to_lowercase();
                    let command: Command = match main_command.as_str().try_into() {
                        Ok(command) => command,
                        Err(e) => {
                            println!("error parsing command: {e:?}");
                            continue;
                        }
                    };
                    let subcommand = tokens[1].trim().to_lowercase();
                    let subcommand: SubCommand = match subcommand.as_str().try_into() {
                        Ok(subcommand) => subcommand,
                        Err(e) => {
                            println!("error parsing subcommand: {e:?}");
                            continue;
                        }
                    };
                    execute_commands(command, subcommand, &api_client, &mut editor).await;
                }
            },
            Err(ReadlineError::Interrupted) => {
                break;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }
    Ok(())
}

fn print_invalid_command_help(command: &str) {
    println!("invalid command: {command}");
    println!("type help or ? to get help with commands");
    println!();
}

fn print_help() {
    println!("supported commands:");
    println!();
    println!("  add tenant - add a new tenant");
    println!("  add source - add a new source");
    println!("  add sink - add a new sink");
    println!("  add pipeline - add an new pipeline");
    println!();
    println!("  update tenant - update an existing tenant");
    println!("  update source - update an existing source");
    println!("  update sink - update an existing sink");
    println!("  update pipeline - update a existing pipeline");
    println!();
    println!("  delete tenant - delete an existing tenant");
    println!("  delete source - delete an existing source");
    println!("  delete sink - delete an existing sink");
    println!("  delete pipeline - delete a new pipeline");
    println!();
    println!("  show tenant - show an existing tenant");
    println!("  show source - show an existing source");
    println!("  show sink - show an existing sink");
    println!("  show pipeline - show a existing pipeline");
    println!();
    println!("  list tenants - list all existing tenants");
    println!("  list sources - list all existing sources");
    println!("  list sinks - list all existing sinks");
    println!("  list pipelines - list all existing pipelines");
    println!();
}

pub fn get_string(editor: &mut DefaultEditor, prompt: &str) -> Result<String, CliError> {
    let s = editor.readline(prompt)?;
    let s = s.trim().to_string();
    Ok(s)
}

pub fn get_id(editor: &mut DefaultEditor, prompt: &str) -> Result<i64, CliError> {
    let id = get_string(editor, prompt)?;
    let id = id.trim().parse()?;
    Ok(id)
}

pub fn get_usize(editor: &mut DefaultEditor, prompt: &str) -> Result<usize, CliError> {
    let usize = get_string(editor, prompt)?;
    let usize = usize.trim().parse()?;
    Ok(usize)
}

pub fn get_u64(editor: &mut DefaultEditor, prompt: &str) -> Result<u64, CliError> {
    let u64 = get_string(editor, prompt)?;
    let u64 = u64.trim().parse()?;
    Ok(u64)
}
