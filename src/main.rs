use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;

        stream.readable().await?;
        loop {
            let mut buf = [0; 512];

            let read = stream.try_read(&mut buf).unwrap_or_default();

            if read == 0 { continue }

            stream.write(b"+PONG\r\n").await?;
        }
    }
}
