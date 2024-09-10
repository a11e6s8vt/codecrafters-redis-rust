#[derive(Debug, Clone, PartialEq)]
pub enum InfoSubCommand {
    Replication,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Info {
    pub sub_command: Option<InfoSubCommand>,
}
