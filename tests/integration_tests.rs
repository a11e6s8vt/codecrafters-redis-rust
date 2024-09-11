use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;

// Import functions from lib.rs
fn start_client(message: &[u8]) -> std::io::Result<String> {
    let mut stream = TcpStream::connect("127.0.0.1:6379")?;
    stream.write(message)?;

    let mut buffer = [0; 512];
    let n = stream.read(&mut buffer)?;
    println!("{:?}", &buffer);
    Ok(String::from_utf8_lossy(&buffer[..n]).to_string())
}

#[test]
fn test_redis_ping() {
    let message = b"*1\r\n$4\r\nPING\r\n";
    let result = b"+PONG\r\n";
    let response = start_client(message).expect("Failed to send/receive message");
    assert_eq!(response, String::from_utf8_lossy(result).to_string());
}

#[test]
fn test_redis_echo() {
    let message = b"*2\r\n$4\r\nECHO\r\n$5\r\nHELLO\r\n";
    let result = b"+HELLO\r\n";
    let response = start_client(message).expect("Failed to send/receive message");
    assert_eq!(response, String::from_utf8_lossy(result).to_string());
}

#[test]
fn test_redis_echo_error() {
    // Not working
    let message = b"*2\r\n$4\r\nECHO\r\n";
    let result = b"+HELLO\r\n";
    let stream = TcpStream::connect("127.0.0.1:6379").unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());

    let mut stream = stream;
    stream.write_all(message).unwrap();
    let mut line = String::new();

    // Read a line from the server using the buffered reader
    match reader.read_line(&mut line) {
        Ok(0) => {
            println!("Connection closed by the server.");
        }
        Ok(_) => {
            println!("Received: {}", line);
        }
        Err(e) => {
            println!("Error reading from socket: {}", e);
        }
    }

    // assert_eq!(response, String::from_utf8_lossy(result).to_string());
}
