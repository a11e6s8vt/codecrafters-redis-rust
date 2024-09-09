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
