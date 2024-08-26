use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut rl = DefaultEditor::new()?;
    println!("replicator api CLI version 0.1.0");
    println!("type help or ? for help");
    println!();
    loop {
        let readline = rl.readline("> ");
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
                    let subcommand = tokens[1].trim().to_lowercase();
                    match main_command.as_ref() {
                        "a" | "ad" | "add" => {
                            handle_subcommand(&subcommand);
                        }
                        "u" | "up" | "upd" | "upda" | "updat" | "update" => {
                            handle_subcommand(&subcommand);
                        }
                        "d" | "de" | "del" | "dele" | "delet" | "delete" => {
                            handle_subcommand(&subcommand);
                        }
                        "s" | "sh" | "sho" | "show" => {
                            handle_subcommand(&subcommand);
                        }
                        "l" | "li" | "lis" | "list" => {
                            // handle subcommand
                            handle_subcommand(&subcommand);
                        }
                        _ => {
                            print_invalid_command_help(command);
                        }
                    }
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

fn handle_subcommand(subcommand: &str) {
    match subcommand {
        "t" | "te" | "ten" | "tena" | "tenan" | "tenant" | "tenants" => {
            println!("tenants:");
        }
        "so" | "sou" | "sour" | "sourc" | "source" | "sources" => {
            println!("sources:");
        }
        "si" | "sin" | "sink" | "sinks" => {
            println!("sinks:");
        }
        "p" | "pi" | "pip" | "pipe" | "pipel" | "pipeli" | "pipelin" | "pipeline" | "pipelines" => {
            println!("pipelines:");
        }
        subcommand => {
            println!("unknown subcommand: {subcommand}");
            if subcommand == "s" {
                println!("'s' is ambiguous between sources and sinks");
            }
        }
    }
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
