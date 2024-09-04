use std::{
    collections::{HashMap, HashSet},
    str::{self, Utf8Error},
};

use bytes::Bytes;

use crate::token::{Token, Tokenizer};

#[derive(Debug)]
pub enum RespError {
    Invalid,
}

impl From<Utf8Error> for RespError {
    fn from(_: Utf8Error) -> Self {
        Self::Invalid
    }
}

#[derive(Clone, Debug)]
pub enum RespData {
    String(String),
    ErrorStr(String),
    Integer(i64),
    BulkStr(Bytes),
    Array(Vec<RespData>),
    Null,
    Boolean(bool),
    Double(f64),
    // BigNum(BigInt),
    BulkError(Bytes),
    VerbatimStr(Bytes),
    Map(HashMap<RespData, RespData>),
    Set(HashSet<RespData>),
}

impl RespData {
    pub fn valid() -> bool {
        unimplemented!()
    }
}

/// Clients send commands to the Redis server as RESP arrays
/// RESP Arrays' encoding uses the following format:
///     *<number-of-elements>\r\n<element-1>...<element-n>
/// "*2\r\n\$3\r\nGET\r\n\$3\r\nfoo\r\n"
impl<'b> TryFrom<Tokenizer<'b>> for RespData {
    type Error = RespError;

    fn try_from(mut tokens: Tokenizer) -> std::result::Result<Self, Self::Error> {
        let first_token = if let Some(Ok(first_token)) = tokens.next() {
            first_token
        } else {
            return Err(RespError::Invalid);
        };

        match first_token {
            Token::Asterisk => {
                let mut res: Vec<RespData> = Vec::new();

                let array_len = if let Some(Ok(second_token)) = tokens.next() {
                    match second_token {
                        Token::Num(i) => i,
                        _ => return Err(RespError::Invalid),
                    }
                } else {
                    return Err(RespError::Invalid);
                };
                // res.push(RespData::Integer(array_len));

                while let Some(tk) = tokens.next() {
                    if let Ok(token) = tk {
                        match token {
                            Token::Dollar => {
                                if let Some(Ok(t)) = tokens.next() {
                                    dbg!(t.clone());
                                    let token_len = match t {
                                        Token::Num(i) => i,
                                        _ => return Err(RespError::Invalid),
                                    };

                                    if let Some(Ok(t)) = tokens.next() {
                                        let word = match t {
                                            Token::Word(w) => w,
                                            Token::Num(w) => w.to_string(),
                                            _ => return Err(RespError::Invalid),
                                        };

                                        if word.len() == token_len as usize {
                                            if let Ok(n) = word.parse::<i64>() {
                                                res.push(RespData::Integer(n))
                                            } else {
                                                res.push(RespData::String(word));
                                            }
                                            dbg!(res.clone());
                                        } else {
                                            return Err(RespError::Invalid);
                                        }
                                    }
                                }
                            }
                            _ => return Err(RespError::Invalid),
                        }
                    } else {
                        return Err(RespError::Invalid);
                    }
                }
                if res.len() == array_len as usize {
                    return Ok(RespData::Array(res));
                } else {
                    return Err(RespError::Invalid);
                }
            }
            _ => {
                return Err(RespError::Invalid);
            }
        }
    }
}
