use std::time::Duration;

use crate::{
    cmds::{Command, CommandError, Config, Echo, Get, Ping, Set, SubCommand},
    resp::RespData,
};

pub fn parse_command(v: Vec<RespData>) -> anyhow::Result<Command, CommandError> {
    dbg!("resp_data parse {:?}", v.clone());
    let mut v_iter = v.iter();
    let cmd_str = if let Some(cmd_str) = v_iter.next() {
        match cmd_str {
            RespData::String(cmd) => Some(cmd.to_owned()),
            _ => None,
        }
    } else {
        None
    };

    if let Some(cmd_name) = cmd_str {
        match cmd_name.to_ascii_lowercase().as_str() {
            "set" => {
                let key = if let Some(RespData::String(key)) = v_iter.next() {
                    key.to_owned()
                } else {
                    return Err(CommandError::WrongNumberOfArguments("set".into()));
                };

                let value = if let Some(RespData::String(value)) = v_iter.next() {
                    value.to_owned()
                } else {
                    return Err(CommandError::WrongNumberOfArguments("set".into()));
                };

                let expiry = match v_iter.next() {
                    Some(RespData::Integer(expiry)) => {
                        Some(Duration::from_millis(expiry.clone() as u64))
                    }
                    Some(_) => {
                        return Err(CommandError::NotValidType("set".into()));
                    }
                    None => None,
                };

                let s = Set { key, value, expiry };

                if let Some(_) = v_iter.next() {
                    return Err(CommandError::SyntaxError("set".into()));
                }

                return Ok(Command::Set(s));
            }
            "get" => {
                let key = if let Some(RespData::String(key)) = v_iter.next() {
                    key.to_owned()
                } else {
                    return Err(CommandError::WrongNumberOfArguments("get".into()));
                };

                if let Some(_) = v_iter.next() {
                    return Err(CommandError::WrongNumberOfArguments("get".into()));
                }

                let g = Get { key };
                return Ok(Command::Get(g));
            }
            "ping" => {
                if let Some(RespData::String(value)) = v_iter.next() {
                    let p = Ping {
                        value: Some(value.to_owned()),
                    };

                    if let Some(_) = v_iter.next() {
                        return Err(CommandError::WrongNumberOfArguments("ping".into()));
                    }
                    return Ok(Command::Ping(p));
                } else {
                    let p = Ping { value: None };
                    return Ok(Command::Ping(p));
                };
            }
            "echo" => {
                if let Some(RespData::String(value)) = v_iter.next() {
                    let e = Echo {
                        value: Some(value.to_owned()),
                    };

                    if let Some(_) = v_iter.next() {
                        return Err(CommandError::WrongNumberOfArguments("echo".into()));
                    }
                    return Ok(Command::Echo(e));
                } else {
                    let e = Echo { value: None };
                    return Ok(Command::Echo(e));
                };
            }
            "config" => {
                let subcommand = if let Some(RespData::String(name)) = v_iter.next() {
                    match name.to_ascii_lowercase().as_str() {
                        "get" => {
                            let pattern = if let Some(RespData::String(pattern)) = v_iter.next() {
                                pattern.to_owned()
                            } else {
                                return Err(CommandError::WrongNumberOfArguments("config".into()));
                            };
                            SubCommand::Get(pattern)
                        }
                        _ => return Err(CommandError::UnknownSubCommand("get".into())),
                    }
                } else {
                    return Err(CommandError::WrongNumberOfArguments("config".into()));
                };

                if let Some(_) = v_iter.next() {
                    return Err(CommandError::SyntaxError("config".into()));
                }

                let s = Config {
                    sub_command: subcommand,
                };

                return Ok(Command::Config(s));
            }
            _ => {}
        }
    }
    return Err(CommandError::NotSupported);
}
