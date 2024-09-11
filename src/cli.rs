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
        let mut listening_port = Some(6379u16);
        let bind_address = Some(String::from("127.0.0.1"));
        let mut replicaof = None;
        while let Some(param) = args.next() {
            match param.to_ascii_lowercase().as_str() {
                "--dir" => {
                    if let Some(s) = args.next() {
                        dir_name = Some(s);
                    }
                }

                "--dbfilename" => {
                    if let Some(s) = args.next() {
                        db_filename = Some(s);
                    }
                }

                "--port" => {
                    let port = match args.next() {
                        Some(s) => Some(s.parse::<u16>().unwrap()),
                        None => Some(6379u16),
                    };
                    listening_port = port;
                }

                "--replicaof" => {
                    if let Some(s) = args.next() {
                        replicaof = Some(s);
                    }
                }
                _ => {}
            }
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
