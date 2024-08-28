use core::net::SocketAddr;
use std::convert::TryFrom;
use std::fmt::{Debug, Display};
use std::io::Result;
use std::str;
use std::str::Utf8Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

const CHUNK_SIZE: usize = 1 * 1024;

pub async fn handle_client(
    mut stream: BufReader<TcpStream>,
    socket_addr: SocketAddr,
) -> Result<String> {
    let mut buffer = [0; CHUNK_SIZE];
    loop {
        let num_bytes = stream.read(&mut buffer).await?;
        match Command::try_from(&buffer[..num_bytes]) {
            Ok(cmd) => match cmd.name.as_str() {
                "echo" => {
                    if cmd.arguments.clone().is_some_and(|x| x.len() == 1) {
                        let arg = cmd.arguments.unwrap();
                        let mut response: String = "+".to_owned();
                        response.push_str(&arg[0].clone());
                        response.push_str("\r\n");
                        stream.write_all(response.as_bytes()).await?;
                    } else {
                        let response = "-ERR wrong number of arguments for 'echo' command\r\n";
                        stream.write_all(response.as_bytes()).await?;
                    }
                }
                "ping" => {
                    let response = "+PONG\r\n";
                    stream.write_all(response.as_bytes()).await?;
                }
                "exit" => {
                    break;
                }
                "command" => {}
                _ => {}
            },
            Err(e) => log::error!("Failed to parse a command: {}", e),
        }
    }
    Ok(socket_addr.ip().to_string())
}

#[derive(Debug, Default)]
pub struct Command {
    pub name: String,
    pub arguments: Option<Vec<String>>,
}

impl Command {
    fn new() -> Self {
        Self::default()
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// Clients send commands to the Redis server as RESP arrays
/// RESP Arrays' encoding uses the following format:
///     *<number-of-elements>\r\n<element-1>...<element-n>
impl TryFrom<&[u8]> for Command {
    type Error = CommandError;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        let mut cmd_str = value
            .split(|&b| b == b'\n')
            .map(|line| line.strip_suffix(b"\r").unwrap_or(line));

        let mut redis_cmd: Vec<String> = Vec::new();
        while let Some(token) = cmd_str.next() {
            let token = str::from_utf8(token)?;
            dbg!(token);
            let token = token.chars().as_str().trim_matches(char::from(0));
            if !(token.starts_with("*") || token.starts_with("$") || token.is_empty()) {
                redis_cmd.push(token.into());
            }
        }
        println!("{:?}", redis_cmd);
        let mut c = Command::new();
        if !redis_cmd.is_empty() {
            c.name = redis_cmd.remove(0).to_ascii_lowercase();
            c.arguments = Some(redis_cmd);
        }
        Ok(c)
    }
}

pub enum CommandError {
    InvalidCommand,
    InvalidEncoding,
    InvalidArgumentsCount,
    InvalidPermission,
}

impl Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl Debug for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl From<Utf8Error> for CommandError {
    fn from(value: Utf8Error) -> Self {
        Self::InvalidEncoding
    }
}

impl CommandError {
    pub fn message(&self) -> &str {
        match self {
            Self::InvalidCommand => "unknown command",
            Self::InvalidEncoding => "invalid encoding",
            Self::InvalidArgumentsCount => "wrong number of arguments for command",
            Self::InvalidPermission => "User has no permissions to run the command",
        }
    }
}
