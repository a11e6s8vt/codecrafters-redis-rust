#[derive(Debug, Clone, PartialEq)]
pub enum SubCommand {
    Get(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub sub_command: SubCommand,
}
