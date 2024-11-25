use tokio::io::AsyncWriteExt;
use tokio_util::codec::FramedRead;
use tokio_stream::StreamExt;

mod resp;
mod cmd;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;

        tokio::spawn(async move {
            loop {
                let mut reader = FramedRead::new(&mut stream, resp::RespParser::default());
                let x = reader.next().await.unwrap().unwrap();
                assert!(matches!(x, resp::RedisValueRef::Array(_)));

                let x = x.to_vec().expect("redis-cli must send array of bulk strings");
                let cmds = cmd::parse_cmds(x).unwrap();

                for cmd in cmds {
                    match cmd {
                        cmd::Cmd::PING => {
                            stream.write(b"+PONG\r\n").await.unwrap();
                        }
                        cmd::Cmd::ECHO(msg) => {
                            stream.write(format!("+{}\r\n", msg).as_bytes()).await.unwrap();
                        }
                        cmd::Cmd::Get(_key) => {
                            todo!()
                        }
                        cmd::Cmd::Set(_key, _val) => {
                            todo!()
                        }
                    }
                }
            }
        });
    }
}
