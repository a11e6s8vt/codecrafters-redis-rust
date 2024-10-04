use std::time::Duration;

use crate::{
    cmds::{
        Command, CommandError, Config, Discard, Echo, Exec, Get, Incr, Info, InfoSubCommand, Keys,
        Multi, Ping, Psync, Replconf, Save, Set, SubCommand, Type, Wait, Xadd, Xrange, Xread,
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
                let key = match v_iter.next() {
                    Some(key) => match key {
                        RespData::String(s) => s.to_owned(),
                        RespData::Integer(n) => n.to_string(),
                        _ => return Err(CommandError::NotValidType("set".into())),
                    },
                    None => return Err(CommandError::WrongNumberOfArguments("set".into())),
                };

                let value = match v_iter.next() {
                    Some(value) => match value {
                        RespData::String(s) => s.to_owned(),
                        RespData::Integer(n) => n.to_string(),
                        _ => return Err(CommandError::NotValidType("set".into())),
                    },
                    None => return Err(CommandError::WrongNumberOfArguments("set".into())),
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

                if v_iter.next().is_some() {
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

                if v_iter.next().is_some() {
                    return Err(CommandError::WrongNumberOfArguments("get".into()));
                }

                let g = Get { key };
                return Ok(Command::Get(g));
            }
            "incr" => {
                let key = if let Some(RespData::String(key)) = v_iter.next() {
                    key.to_owned()
                } else {
                    return Err(CommandError::WrongNumberOfArguments("INCR".into()));
                };

                if v_iter.next().is_some() {
                    return Err(CommandError::WrongNumberOfArguments("INCR".into()));
                }

                let g = Incr { key };
                return Ok(Command::Incr(g));
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

                    if v_iter.next().is_some() {
                        return Err(CommandError::WrongNumberOfArguments("echo".into()));
                    }
                    return Ok(Command::Echo(e));
                } else {
                    let e = Echo { value: None };
                    return Ok(Command::Echo(e));
                };
            }
            "exec" => {
                if v_iter.next().is_some() {
                    return Err(CommandError::WrongNumberOfArguments("exec".into()));
                }
                return Ok(Command::Exec(Exec));
            }
            "multi" => {
                if v_iter.next().is_some() {
                    return Err(CommandError::WrongNumberOfArguments("multi".into()));
                }
                return Ok(Command::Multi(Multi));
            }
            "discard" => {
                if v_iter.next().is_some() {
                    return Err(CommandError::WrongNumberOfArguments("discard".into()));
                }
                return Ok(Command::Discard(Discard));
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
            "replconf" => match v_iter.next() {
                Some(RespData::String(s)) => match s.to_ascii_lowercase().as_str() {
                    "listening-port" => {
                        let port = if let Some(RespData::Integer(port)) = v_iter.next() {
                            port
                        } else {
                            return Err(CommandError::NotValidType("replconf".into()));
                        };

                        if v_iter.next().is_some() {
                            return Err(CommandError::WrongNumberOfArguments("replconf".into()));
                        }
                        return Ok(Command::Replconf(Replconf {
                            args: vec!["listening-port".into(), port.to_string()],
                        }));
                    }
                    "capa" => {
                        let mut args: Vec<String> = vec!["capa".into()];
                        if let Some(RespData::String(s)) = v_iter.next() {
                            args.push(s.to_string());
                        }
                        return Ok(Command::Replconf(Replconf { args }));
                    }
                    "getack" => {
                        let mut args: Vec<String> = vec!["getack".into()];
                        if let Some(RespData::String(s)) = v_iter.next() {
                            args.push(s.to_string());
                        }
                        return Ok(Command::Replconf(Replconf { args }));
                    }
                    "ack" => {
                        let mut args: Vec<String> = vec!["ack".into()];
                        if let Some(RespData::Integer(s)) = v_iter.next() {
                            args.push(s.to_string());
                        }
                        return Ok(Command::Replconf(Replconf { args }));
                    }
                    _ => {}
                },
                Some(_) => {}
                None => todo!(),
            },
            "psync" => {
                let mut args: Vec<String> = Vec::new();
                if let Some(RespData::String(s)) = v_iter.next() {
                    args.push(s.to_string());
                }

                if let Some(RespData::Integer(s)) = v_iter.next() {
                    args.push(s.to_string());
                }
                if let Some(_) = v_iter.next() {
                    return Err(CommandError::WrongNumberOfArguments("psync".into()));
                }
                return Ok(Command::Psync(Psync { args }));
            }
            "type" => {
                let key = if let Some(RespData::String(key)) = v_iter.next() {
                    key.to_owned()
                } else {
                    return Err(CommandError::WrongNumberOfArguments("type".into()));
                };

                if let Some(_) = v_iter.next() {
                    return Err(CommandError::WrongNumberOfArguments("type".into()));
                }

                let g = Type { key };
                return Ok(Command::Type(g));
            }
            "wait" => {
                let mut args: Vec<String> = Vec::new();
                if let Some(RespData::Integer(s)) = v_iter.next() {
                    args.push(s.to_string());
                }

                if let Some(RespData::Integer(s)) = v_iter.next() {
                    args.push(s.to_string());
                }
                if let Some(_) = v_iter.next() {
                    return Err(CommandError::WrongNumberOfArguments("psync".into()));
                }
                return Ok(Command::Wait(Wait { args }));
            }
            "xadd" => {
                let mut args: Vec<(String, String)> = Vec::new();
                let key = if let Some(RespData::String(s)) = v_iter.next() {
                    s.to_string()
                } else {
                    return Err(CommandError::NotValidType("XADD".into()));
                };

                let entry_id = if let Some(RespData::String(s)) = v_iter.next() {
                    s.to_string()
                } else {
                    return Err(CommandError::NotValidType("XADD".into()));
                };

                let kv_pairs = v_iter.collect::<Vec<&RespData>>();
                if kv_pairs.len() % 2 != 0 {
                    return Err(CommandError::WrongNumberOfArguments("xread".into()));
                }

                let kv_pairs = kv_pairs
                    .chunks(2)
                    .map(|chunk| {
                        let chunk = chunk.to_vec();
                        (chunk[0].to_owned(), chunk[1].to_owned())
                    })
                    .collect::<Vec<(RespData, RespData)>>();

                for (k, v) in kv_pairs {
                    match (k, v) {
                        (RespData::String(k), RespData::String(v)) => args.push((k, v)),
                        (RespData::String(k), RespData::Integer(v)) => {
                            args.push((k, v.to_string()))
                        }
                        (_, _) => return Err(CommandError::NotValidType("XADD".into())),
                    }
                }

                return Ok(Command::Xadd(Xadd {
                    key,
                    entry_id,
                    args,
                }));
            }
            "xrange" => {
                let key = if let Some(RespData::String(s)) = v_iter.next() {
                    s.to_string()
                } else {
                    return Err(CommandError::NotValidType("XRANGE".into()));
                };

                let start = if let Some(RespData::String(s)) = v_iter.next() {
                    s.to_string()
                } else {
                    return Err(CommandError::NotValidType("XRANGE".into()));
                };

                let end = if let Some(RespData::String(s)) = v_iter.next() {
                    s.to_string()
                } else {
                    return Err(CommandError::NotValidType("XRANGE".into()));
                };

                if let Some(_) = v_iter.next() {
                    return Err(CommandError::WrongNumberOfArguments("xrange".into()));
                }

                return Ok(Command::Xrange(Xrange { key, start, end }));
            }
            "xread" => {
                let mut block: Option<u64> = None;
                let mut cmd_options: Vec<String> = Vec::new();
                loop {
                    let cmd = match v_iter.next() {
                        Some(RespData::String(s)) => s.to_string(),
                        Some(_) => return Err(CommandError::NotValidType("XREAD".into())),
                        None => return Err(CommandError::NotValidType("XREAD".into())),
                    };
                    match cmd.to_ascii_lowercase().as_str() {
                        "block" => {
                            if let Some(RespData::Integer(n)) = v_iter.next() {
                                block = Some(n.clone() as u64);
                            }
                        }
                        "streams" => {
                            while let Some(d) = v_iter.next() {
                                match d {
                                    RespData::String(s) => {
                                        cmd_options.push(s.to_string());
                                    }
                                    RespData::Integer(s) => {
                                        cmd_options.push(s.to_string());
                                    }
                                    _ => {
                                        return Err(CommandError::WrongNumberOfArguments(
                                            "XREAD".into(),
                                        ))
                                    }
                                }
                            }
                            if cmd_options.len() % 2 != 0 {
                                return Err(CommandError::WrongNumberOfArguments("XREAD".into()));
                            } else {
                                break;
                            }
                        }
                        _ => break,
                    }
                }

                let mid = cmd_options.len() / 2;
                let keys: Vec<_> = cmd_options[..mid].to_vec();
                let entry_ids: Vec<_> = cmd_options[mid..].to_vec();
                let cmd = Command::Xread(Xread {
                    block,
                    keys,
                    entry_ids,
                });
                return Ok(cmd);
            }
            _ => {}
        }
    }
    return Err(CommandError::NotSupported);
}
