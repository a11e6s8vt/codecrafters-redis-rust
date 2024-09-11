use std::env::Args;

#[derive(Clone, Debug)]
pub struct Cli {
    pub listening_port: Option<u16>,
    pub bind_address: Option<String>,
    pub dir_name: Option<String>,
    pub db_filename: Option<String>,
    pub replicaof: Option<String>,
}

// impl Display for Cli {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(
//             f,
//             "RDB Dir: {}\nRDB Filename: {}\nBind Address: {}\nPort: {}\n",
//             self.dir_name, self.db_filename, self.bind_address, self.listening_port
//         )
//     }
// }

impl Cli {
    pub fn new(mut args: Args) -> Self {
        let mut dir_name = None;
        let mut db_filename = None;
        let mut listening_port = None;
        let bind_address = Some(String::from("127.0.0.1"));
        let mut replicaof = None;
        while let Some(param) = args.next() {
            dir_name = if param.eq_ignore_ascii_case("--dir".into()) {
                if let Some(s) = args.next() {
                    Some(s)
                } else {
                    None
                }
            } else {
                None
            };

            db_filename = if param.eq_ignore_ascii_case("--dbfilename".into()) {
                if let Some(s) = args.next() {
                    Some(s)
                } else {
                    None
                }
            } else {
                None
            };

            listening_port = if param.eq_ignore_ascii_case("--port".into()) {
                if let Some(s) = args.next() {
                    if let Ok(port) = s.parse::<u16>() {
                        Some(port)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };

            replicaof = if param.eq_ignore_ascii_case("--replicaof".into()) {
                if let Some(s) = args.next() {
                    Some(s)
                } else {
                    None
                }
            } else {
                None
            };
        }

        Self {
            listening_port,
            bind_address,
            dir_name,
            db_filename,
            replicaof,
        }
    }
}
