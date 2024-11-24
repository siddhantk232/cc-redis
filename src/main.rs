use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;

        tokio::spawn(async move {
            loop {
                let mut buf = [0; 512];

                let read = match stream.read(&mut buf).await {
                    Ok(r) => r,
                    Err(_e) => continue,
                };

                if read == 0 {
                    return Ok::<(), std::io::Error>(());
                }

                stream.write(b"+PONG\r\n").await?;
            }
        });
    }
}
