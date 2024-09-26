use std::{
    collections::{HashMap, HashSet},
    str::{self, Chars, Utf8Error},
};

use bytes::Bytes;
use std::iter::Peekable;

type IT<'b> = Peekable<Chars<'b>>;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Asterisk,
    CRLF,
    Dollar,
    Plus,
    Minus,
    Colon,
    Underscore,
    Comma,
    PercentSign,
    BracketOpen,
    Exclamation,
    EqualSign,
    Tilde,
    GreaterThan,
    Question,
    Num(i64),
    Word(String),
    NewLine,
    CarriageReturn,
}

#[derive(Debug, Clone)]
pub struct Tokenizer<'b> {
    it: IT<'b>,
}

impl<'b> Tokenizer<'b> {
    pub fn new(s: &'b String) -> Result<Self, Utf8Error> {
        Ok(Tokenizer {
            it: s.chars().peekable(),
        })
    }
}

impl<'b> Iterator for Tokenizer<'b> {
    type Item = Result<Token, String>;
    // *2\r\n\$3\r\nGET\r\n\$3\r\nfoo\r\n
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(c) = self.it.next() {
            match c {
                '\r' | '\n' => return Some(Ok(Token::CRLF)),
                ':' => return Some(Ok(Token::Colon)),
                '_' => return Some(Ok(Token::Underscore)),
                '-' => {
                    let t = utf8_token(&mut self.it, c);
                    if let Ok(num) = t.parse::<i64>() {
                        return Some(Ok(Token::Num(num)));
                    } else {
                        return Some(Ok(Token::Word(t)));
                    }
                }
                '+' => return Some(Ok(Token::Plus)),
                '*' => return Some(Ok(Token::Asterisk)),
                '$' => return Some(Ok(Token::Dollar)),
                ',' => return Some(Ok(Token::Comma)),
                '%' => return Some(Ok(Token::PercentSign)),
                '(' => return Some(Ok(Token::BracketOpen)),
                '!' => return Some(Ok(Token::Exclamation)),
                '=' => return Some(Ok(Token::EqualSign)),
                '~' => return Some(Ok(Token::Tilde)),
                '>' => return Some(Ok(Token::GreaterThan)),
                '?' => return Some(Ok(Token::Question)),
                v => {
                    let t = utf8_token(&mut self.it, v);
                    if let Ok(num) = t.parse::<i64>() {
                        return Some(Ok(Token::Num(num)));
                    } else {
                        return Some(Ok(Token::Word(t)));
                    }
                }
            }
        }
        None
    }
}

fn utf8_token(it: &mut IT, c: char) -> String {
    let mut word = c.to_owned().to_string();

    while let Some(c) = it.next() {
        if c == '\r' {
            it.next();
            //it.next();
            break;
        }
        word.push(c);
    }

    word
}

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
    pub fn parse(resp_str: &String) -> anyhow::Result<Vec<RespData>, RespError> {
        let mut result: Vec<RespData> = Vec::new();
        if let Ok(mut tk) = Tokenizer::new(resp_str) {
            while let Some(Ok(t)) = tk.next() {
                match t {
                    Token::Asterisk => {
                        // RESP Array
                        let mut res: Vec<RespData> = Vec::new();
                        let array_length = if let Some(Ok(Token::Num(array_length))) = tk.next() {
                            array_length
                        } else {
                            break;
                        };
                        while let Some(Ok(token)) = tk.next() {
                            match token {
                                Token::Dollar => {
                                    let word_len = if let Some(Ok(Token::Num(word_len))) = tk.next()
                                    {
                                        word_len
                                    } else {
                                        break;
                                    };
                                    let word = match tk.next() {
                                        Some(Ok(Token::Word(word))) => word,
                                        Some(Ok(Token::Num(num))) => num.to_string(),
                                        Some(Ok(Token::Question)) => "?".to_string(),
                                        Some(Ok(Token::Asterisk)) => "*".to_string(),
                                        Some(Ok(Token::Minus)) => "-".to_string(),
                                        Some(Ok(_)) => break,
                                        Some(Err(_)) => break,
                                        None => break,
                                    };
                                    dbg!(&word);

                                    if word.len() == word_len as usize {
                                        if let Ok(n) = word.parse::<i64>() {
                                            res.push(RespData::Integer(n));
                                        } else {
                                            res.push(RespData::String(word));
                                        }
                                    } else {
                                        return Err(RespError::Invalid);
                                    }

                                    if res.len() == array_length as usize {
                                        result.push(RespData::Array(res));
                                        break;
                                    }
                                }
                                Token::CRLF => {}
                                _ => todo!(),
                            }
                        }
                    }
                    Token::CRLF => {}
                    Token::Dollar => {}
                    Token::Plus => {
                        let mut res: Vec<RespData> = Vec::new();
                        while let Some(Ok(token)) = tk.next() {
                            match token {
                                Token::Word(word) => {
                                    res.push(RespData::String(word));
                                }
                                Token::CRLF => break,
                                _ => {}
                            }
                        }
                        result.push(RespData::Array(res));
                    }
                    Token::Minus => todo!(),
                    Token::Colon => todo!(),
                    Token::Underscore => todo!(),
                    Token::Comma => todo!(),
                    Token::PercentSign => todo!(),
                    Token::BracketOpen => todo!(),
                    Token::Exclamation => todo!(),
                    Token::EqualSign => todo!(),
                    Token::Tilde => todo!(),
                    Token::GreaterThan => todo!(),
                    Token::Question => todo!(),
                    Token::Num(num) => {
                        result.push(RespData::Integer(num));
                    }
                    Token::Word(_) => {}
                    Token::NewLine => todo!(),
                    Token::CarriageReturn => todo!(),
                }
            }
        }
        Ok(result)
    }
}

pub fn parse_handshake_response(resp_str: &String) -> Vec<Vec<String>> {
    let mut result: Vec<Vec<String>> = Vec::new();
    if let Ok(mut tk) = Tokenizer::new(resp_str) {
        while let Some(Ok(t)) = tk.next() {
            match t {
                Token::Asterisk => {}
                Token::CRLF => {}
                Token::Dollar => todo!(),
                Token::Plus => {
                    let mut res: Vec<String> = Vec::new();
                    while let Some(Ok(token)) = tk.next() {
                        match token {
                            Token::Word(word) => {
                                res.push(word);
                            }
                            Token::CRLF => break,
                            _ => {}
                        }
                    }
                    result.push(res);
                }
                Token::Minus => todo!(),
                Token::Colon => todo!(),
                Token::Underscore => todo!(),
                Token::Comma => todo!(),
                Token::PercentSign => todo!(),
                Token::BracketOpen => todo!(),
                Token::Exclamation => todo!(),
                Token::EqualSign => todo!(),
                Token::Tilde => todo!(),
                Token::GreaterThan => todo!(),
                Token::Question => todo!(),
                Token::Num(_) => todo!(),
                Token::Word(_) => todo!(),
                Token::NewLine => todo!(),
                Token::CarriageReturn => todo!(),
            }
        }
    } else {
        println!("Creating tokenizer failed");
    }
    result
}
