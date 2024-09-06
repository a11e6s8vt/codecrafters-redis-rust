use std::{env::Args, fmt::Display};

#[derive(Clone, Debug)]
pub struct Cli {
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
        }

        Self {
            dir_name,
            db_filename,
        }
    }
}
