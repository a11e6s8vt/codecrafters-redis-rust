use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub struct Get {
    pub key: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Set {
    pub key: String,
    pub value: String,
    pub expiry: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Ping {
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Echo {
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Get(Get),
    Set(Set),
    Ping(Ping),
    Echo(Echo),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CommandError {
    SyntaxError(String),
    WrongNumberOfArguments(String),
    NotSupported,
    NotValidType { cmd: String, arg: String },
}

impl CommandError {
    pub fn message(&self) -> String {
        match self {
            Self::SyntaxError(x) => format!("ERR syntax error"),
            Self::WrongNumberOfArguments(x) => {
                format!("ERR wrong number of arguments for '{}' command", x)
            }
            Self::NotSupported => format!("ERR Command Not Supported"),
            Self::NotValidType { cmd, arg } => {
                format!(
                    "ERR '{}' is not a valid type for the command '{}'",
                    arg, cmd
                )
            }
        }
    }
}
