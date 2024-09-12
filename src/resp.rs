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

// todo:
// impl PartialEq for RespData {
//     fn eq(&self, other: &Self) -> bool {}

//     fn ne(&self, other: &Self) -> bool {
//         !self.eq(other)
//     }
// }

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
        dbg!(first_token.clone());

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

                while let Some(tk) = tokens.next() {
                    if let Ok(token) = tk {
                        dbg!(token.clone());
                        match token {
                            Token::Dollar => {
                                if let Some(Ok(t)) = tokens.next() {
                                    let token_len = match t {
                                        Token::Num(i) => i,
                                        _ => return Err(RespError::Invalid),
                                    };

                                    if let Some(Ok(t)) = tokens.next() {
                                        let mut word = match t {
                                            Token::Word(w) => w,
                                            Token::Num(n) => n.to_string(),
                                            Token::Asterisk => "*".to_string(),
                                            // Token::Minus => "-".to_string(),
                                            Token::Question => "?".to_string(),
                                            _ => return Err(RespError::Invalid),
                                        };
                                        // Special case - "listening-port"
                                        if word.as_str() == "listening" {
                                            if let Some(RespData::String(kw)) = res.last() {
                                                if kw.as_str() == "REPLCONF" {
                                                    let (token1, token2) =
                                                        (tokens.next(), tokens.next());
                                                    if let Some(Ok(Token::Minus)) = token1 {
                                                        dbg!(Token::Minus);
                                                        if let Some(Ok(Token::Word(t))) = token2 {
                                                            if t.as_str() == "port" {
                                                                word = "listening-port".to_string();
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        // Special case - "psync2" - alphanumeric
                                        // todo: rewrite tokenizer to handle alphanumeric
                                        if word.as_str() == "psync" {
                                            if let Some(RespData::String(kw)) = res.first() {
                                                if kw.as_str() == "REPLCONF" {
                                                    let token1 = tokens.next();
                                                    if let Some(Ok(Token::Num(n))) = token1 {
                                                        if n == 2 {
                                                            word = "psync2".to_string();
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        dbg!(word.clone());
                                        if word.len() == token_len as usize {
                                            if let Ok(n) = word.parse::<i64>() {
                                                res.push(RespData::Integer(n));
                                            } else {
                                                res.push(RespData::String(word));
                                            }
                                        } else {
                                            return Err(RespError::Invalid);
                                        }
                                    }
                                }
                            }
                            Token::Colon => {
                                if let Some(Ok(t)) = tokens.next() {
                                    let n = match t {
                                        Token::Num(w) => w,
                                        _ => return Err(RespError::Invalid),
                                    };

                                    res.push(RespData::Integer(n));
                                } else {
                                    return Err(RespError::Invalid);
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
