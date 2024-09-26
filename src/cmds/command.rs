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
pub struct Save;

#[derive(Debug, Clone, PartialEq)]
pub struct Keys {
    pub arg: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Replconf {
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Psync {
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Wait {
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Type {
    pub key: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Xadd {
    pub key: String,
    pub entry_id: String,
    pub args: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Xrange {
    pub key: String,
    pub start: String,
    pub end: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Xread {
    pub keys: Vec<String>,
    pub entry_ids: Vec<String>,
}
