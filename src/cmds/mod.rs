pub use command::{Echo, Get, Ping, Save, Set};
pub use config::{Config, SubCommand};

mod command;
mod config;

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Get(Get),
    Set(Set),
    Ping(Ping),
    Echo(Echo),
    Config(Config),
    Save(Save),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CommandError {
    SyntaxError(String),
    WrongNumberOfArguments(String),
    NotSupported,
    NotValidType(String),
    UnknownSubCommand(String),
}

impl CommandError {
    pub fn message(&self) -> String {
        match self {
            Self::SyntaxError(x) => format!("ERR syntax error"),
            Self::WrongNumberOfArguments(x) => {
                format!("ERR wrong number of arguments for '{}' command", x)
            }
            Self::NotSupported => format!("ERR Command Not Supported"),
            Self::NotValidType(x) => {
                format!("ERR Not a valid type for the command '{}'", x)
            }
            Self::UnknownSubCommand(x) => format!("ERR Unknown subcommand '{}'", x),
        }
    }
}
