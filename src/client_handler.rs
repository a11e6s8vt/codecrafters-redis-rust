use crate::command::{Command, CommandError};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::connection::Connection;
use crate::execute::execute_command;
use crate::resp::RespData;

pub async fn handle_client<'a>(
    conn: &mut Connection<'a>,
    db: Arc<Mutex<HashMap<String, Bytes>>>,
) -> anyhow::Result<()> {
    if let Ok(Some(resp_data)) = conn.read().await {
        match resp_data {
            RespData::Array(v) => match execute_command(v) {
                Ok(res) => match res {
                    Command::Ping(o) => {
                        let mut response = String::new();
                        {
                            if o.value.is_some() {
                                response.push_str(&format!("+{}", o.value.unwrap()));
                            } else {
                                response.push_str("+PONG");
                            }
                        }
                        response.push_str("\r\n");
                        conn.write(response.as_bytes()).await;
                        return Ok(());
                    }
                    Command::Echo(o) => {
                        let mut response = String::new();
                        {
                            if o.value.is_some() {
                                response.push_str(&format!("+{}", o.value.unwrap()));
                            } else {
                                response.push_str(
                                    "-Error ERR wrong number of arguments for 'echo' command",
                                );
                            }
                        }
                        response.push_str("\r\n");
                        conn.write(response.as_bytes()).await;
                        return Ok(());
                    }
                    Command::Get(o) => {
                        let mut response: String = String::new();
                        {
                            let key = o.key;
                            if let Ok(db) = db.lock() {
                                if db.contains_key(&key) {
                                    let value =
                                        String::from_utf8(db.get(&key).unwrap().to_vec()).unwrap();
                                    response.push_str("$");
                                    response.push_str(&value.len().to_string());
                                    response.push_str("\r\n");
                                    response.push_str(&value);
                                } else {
                                    response.push_str("$-1");
                                }
                            } else {
                                response.push_str("$-1");
                                log::error!("Accessing the DB failed!");
                            }
                        }
                        response.push_str("\r\n");
                        log::error!("response = {:?}", response);
                        let _ = conn.write(response.as_bytes()).await;
                        return Ok(());
                    }
                    Command::Set(o) => {
                        let mut response = String::new();
                        {
                            let key = o.key;
                            let value = o.value;
                            if let Ok(mut db) = db.lock() {
                                db.insert(key, Bytes::from(value));
                                response.push_str(&format!("+OK\r\n"));
                            } else {
                                response.push_str(&format!("-Error 'SET' command failed\r\n"));
                                log::error!("Inserting into the db failed");
                            }
                        }

                        let _ = conn.write(response.as_bytes()).await;
                        return Ok(());
                    }
                },
                Err(e) => match e.clone() {
                    CommandError::SyntaxError(n) => {
                        let mut response: String = "-".to_owned();
                        response.push_str(&e.message());
                        response.push_str("\r\n");
                        let _ = conn.write(response.as_bytes()).await;
                    }
                    CommandError::WrongNumberOfArguments(n) => {
                        let mut response: String = "-".to_owned();
                        response.push_str(&e.message());
                        response.push_str("\r\n");
                        let _ = conn.write(response.as_bytes()).await;
                    }
                    CommandError::NotSupported => {
                        let mut response: String = "-".to_owned();
                        response.push_str(&e.message());
                        response.push_str("\r\n");
                        let _ = conn.write(response.as_bytes()).await;
                    }
                },
            },
            _ => {}
        };
    }
    Ok(())
}
