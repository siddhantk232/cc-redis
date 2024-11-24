use std::io::{Read, Write};
use std::net::TcpListener;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut input = String::new();
                stream.read_to_string(&mut input).unwrap();

                let input = input.to_lowercase();

                for cmd in input.split('\n') {
                    match cmd {
                        "ping" => {
                            stream.write_all(b"+PONG\r\n").unwrap();
                        }
                        _ => {
                            todo!()
                        }
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
