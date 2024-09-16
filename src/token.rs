use num::BigInt;
use std::iter::Peekable;
use std::str::{Chars, Utf8Error};

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
    BigNum(BigInt),
    Word(String),
    NewLine,
    CarriageReturn,
}

#[derive(Debug, Clone)]
pub struct Tokenizer<'b> {
    it: IT<'b>,
}

impl<'b> Tokenizer<'b> {
    pub fn new(s: &'b [u8]) -> Result<Self, Utf8Error> {
        let s = std::str::from_utf8(&s)?;
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
                '\r' | '\n' => {}
                ':' => return Some(Ok(Token::Colon)),
                '_' => return Some(Ok(Token::Underscore)),
                '-' => {
                    if let Some(num) = negative_num_token(&mut self.it) {
                        return Some(Ok(Token::Num(-num)));
                    } else {
                        return Some(Ok(Token::Minus));
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
                v if v.is_alphanumeric() => {
                    let t = alphanumeric_token(&mut self.it, c);
                    if let Ok(num) = t.parse::<i64>() {
                        return Some(Ok(Token::Num(num)));
                    }
                    // // Check if the string is alphanumeric
                    // else if t.chars().all(char::is_alphanumeric) {
                    //     println!("The string is alphanumeric.");
                    // }
                    // // Check if the string is alphabetical
                    // else if s.chars().all(char::is_alphabetic) {
                    //     println!("The string is alphabetical.");
                    // }
                    // If none of the above
                    else {
                        return Some(Ok(Token::Word(t)));
                    }
                }
                c => return Some(Err(format!("unexpected char '{}'", c))),
            }
        }
        None
    }
}

fn negative_num_token(it: &mut IT) -> Option<i64> {
    if let Some(n) = it.peek() {
        if !n.is_ascii_digit() {
            return None;
        }
    }

    let mut num: i64 = 0;

    if let Some(c) = it.next() {
        num = c as i64 - 48;
    }

    while let Some(c) = it.peek() {
        if c.is_digit(10) {
            num = num * 10 + *c as i64 - 48;
        } else {
            return Some(num);
        }

        it.next();
    }

    Some(num)
}

fn alphanumeric_token(it: &mut IT, c: char) -> String {
    let mut word = c.to_owned().to_string();

    while let Some(c) = it.peek() {
        if c.is_ascii_alphanumeric() {
            word.push(*c);
        } else {
            return word;
        }
        it.next();
    }

    word
}

fn num_token(it: &mut IT, c: char) -> i64 {
    let mut num = c as i64 - 48;

    while let Some(c) = it.peek() {
        if c.is_digit(10) {
            num = num * 10 + *c as i64 - 48;
        } else {
            return num;
        }

        it.next();
    }

    num
}

fn word_token(it: &mut IT, c: char) -> String {
    let mut word = c.to_owned().to_string();

    while let Some(c) = it.peek() {
        if c.is_alphabetic() {
            word.push(*c);
        } else {
            return word;
        }
        it.next();
    }

    word
}

#[cfg(test)]
mod tests {
    use crate::token::*;

    #[test]
    fn test_tokenizer() {
        //
        // "*2\r\n\$3\r\nGET\r\n\$3\r\nfoo\r\n" == buffer (byte array over the network)
        let buffer = [
            42, 50, 13, 10, 36, 51, 13, 10, 71, 69, 84, 13, 10, 36, 51, 13, 10, 102, 111, 111, 13,
            10, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let mut tk = Tokenizer::new(&buffer).unwrap();
        assert_eq!(tk.next(), Some(Ok(Token::Asterisk)));
        assert_eq!(tk.next(), Some(Ok(Token::Num(2))));
        assert_eq!(tk.next(), Some(Ok(Token::Dollar)));
        assert_eq!(tk.next(), Some(Ok(Token::Num(3))));
        assert_eq!(tk.next(), Some(Ok(Token::Word("GET".to_string()))));
        assert_eq!(tk.next(), Some(Ok(Token::Dollar)));
        assert_eq!(tk.next(), Some(Ok(Token::Num(3))));
        assert_eq!(tk.next(), Some(Ok(Token::Word("foo".to_string()))));
    }
}
