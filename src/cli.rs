use std::{env::Args, fmt::Display};

#[derive(Clone, Debug)]
pub struct Cli {
    pub listening_port: u16,
    pub bind_address: String,
    pub dir_name: String,
    pub db_filename: String,
}

impl Display for Cli {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.dir_name, self.db_filename)
    }
}

impl Cli {
    pub fn new(mut args: Args) -> Self {
        let mut dir_name = String::new();
        let mut db_filename = String::new();
        let mut listening_port = 6379u16;
        let mut bind_address = String::from("127.0.0.1");
        while let Some(param) = args.next() {
            if param.eq_ignore_ascii_case("--dir".into()) {
                if let Some(s) = args.next() {
                    dir_name.push_str(&s);
                }
            }

            if param.eq_ignore_ascii_case("--dbfilename".into()) {
                if let Some(s) = args.next() {
                    db_filename.push_str(&s);
                }
            }

            if param.eq_ignore_ascii_case("--port".into()) {
                if let Some(s) = args.next() {
                    if let Ok(port) = s.parse::<u16>() {
                        listening_port = port;
                    }
                }
            }
        }

        Self {
            listening_port,
            bind_address,
            dir_name,
            db_filename,
        }
    }
}
