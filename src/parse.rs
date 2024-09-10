use std::time::Duration;

use crate::{
    cmds::{
        Command, CommandError, Config, Echo, Get, Info, InfoSubCommand, Keys, Ping, Save, Set,
        SubCommand,
    },
    resp::RespData,
};

pub fn parse_command(v: Vec<RespData>) -> anyhow::Result<Command, CommandError> {
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

                let mut expiry: Option<Duration> = None;
                match v_iter.next() {
                    Some(RespData::String(nt)) => match nt.to_ascii_lowercase().as_str() {
                        "ex" | "px" => {
                            expiry = match v_iter.next() {
                                Some(RespData::Integer(expiry)) => {
                                    let t = if nt == "ex" {
                                        Duration::from_secs(expiry.clone() as u64)
                                    } else {
                                        Duration::from_millis(expiry.clone() as u64)
                                    };
                                    Some(Duration::new(t.as_secs(), t.subsec_nanos()))
                                }
                                Some(_) => {
                                    return Err(CommandError::NotValidType("set".into()));
                                }
                                None => return Err(CommandError::SyntaxError("set".into())),
                            };
                        }
                        "nx" => todo!(),
                        "xx" => todo!(),
                        "keepttl" => todo!(),
                        _ => return Err(CommandError::SyntaxError("set".into())),
                    },
                    Some(_) => {
                        return Err(CommandError::NotValidType("set".into()));
                    }
                    None => {}
                }

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
            "keys" => {
                if let Some(RespData::String(arg)) = v_iter.next() {
                    let p = Keys {
                        arg: arg.to_owned(),
                    };
                    return Ok(Command::Keys(p));
                } else {
                    return Err(CommandError::SyntaxError("KEYS".into()));
                }
            }
            "info" => {
                let sub_command = if let Some(RespData::String(sub_command)) = v_iter.next() {
                    Some(sub_command)
                } else {
                    None
                };

                let s = if sub_command.is_some() {
                    Info {
                        sub_command: Some(InfoSubCommand::Replication),
                    }
                } else {
                    Info { sub_command: None }
                };

                return Ok(Command::Info(s));
            }
            "save" => {
                let o = Save;
                return Ok(Command::Save(o));
            }
            _ => {}
        }
    }
    return Err(CommandError::NotSupported);
}
