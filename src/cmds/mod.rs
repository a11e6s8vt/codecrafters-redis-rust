pub use command::{
    Echo, Exec, Get, Incr, Keys, Multi, Ping, Psync, Replconf, Save, Set, Type, Wait, Xadd, Xrange,
    Xread,
};
pub use config::{Config, SubCommand};
pub use info::{Info, InfoSubCommand};

mod command;
mod config;
mod info;

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Get(Get),
    Set(Set),
    Incr(Incr),
    Ping(Ping),
    Echo(Echo),
    Multi(Multi),
    Config(Config),
    Exec(Exec),
    Save(Save),
    Keys(Keys),
    Info(Info),
    Replconf(Replconf),
    Psync(Psync),
    Type(Type),
    Wait(Wait),
    Xadd(Xadd),
    Xrange(Xrange),
    Xread(Xread),
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
