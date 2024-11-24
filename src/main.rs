use tokio::io::{AsyncWriteExt, BufReader};

mod resp;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;

        tokio::spawn(async move {
            loop {
                let buf = BufReader::new(&mut stream);
                let cmds = resp::parse(buf).await.unwrap();

                for cmd in cmds {
                    match cmd {
                        resp::Cmd::PING => {
                            stream.write(b"+PONG\r\n").await.unwrap();
                        }
                        resp::Cmd::ECHO(msg) => {
                            stream.write(format!("+{}\r\n", msg).as_bytes()).await.unwrap();
                        }
                    }
                }
            }
        });
    }
}
