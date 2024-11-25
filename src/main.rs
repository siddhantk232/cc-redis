use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use tokio_util::codec::{Encoder, FramedRead};

mod cmd;
mod resp;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let store = Arc::new(scc::HashMap::<String, String>::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;
        let store = store.clone();

        tokio::spawn(async move {
            loop {
                let mut reader = FramedRead::new(&mut stream, resp::RespParser::default());
                let x = reader.next().await.unwrap().unwrap();
                assert!(matches!(x, resp::RedisValueRef::Array(_)));

                let x = x
                    .to_vec()
                    .expect("redis-cli must send array of bulk strings");
                let cmds = cmd::parse_cmds(x).unwrap();

                for cmd in cmds {
                    match cmd {
                        cmd::Cmd::PING => {
                            stream.write(b"+PONG\r\n").await.unwrap();
                        }
                        cmd::Cmd::ECHO(msg) => {
                            stream
                                .write(format!("+{}\r\n", msg).as_bytes())
                                .await
                                .unwrap();
                        }
                        cmd::Cmd::Get(key) => {
                            let mut encoder = resp::RespParser::default();
                            let mut response = Default::default();

                            match store.get_async(&key).await {
                                Some(val) => {
                                    let val = val.get().to_owned();
                                    encoder
                                        .encode(
                                            resp::RedisValueRef::String(val.into()),
                                            &mut response,
                                        )
                                        .unwrap();
                                }
                                None => {
                                    encoder
                                        .encode(resp::RedisValueRef::NullBulkString, &mut response)
                                        .unwrap();
                                }
                            };

                            stream.write(&response).await.unwrap();
                        }
                        cmd::Cmd::Set(key, val) => {
                            let _ = store.insert_async(key, val).await;
                            stream.write(b"+OK\r\n").await.unwrap();
                        }
                    }
                }
            }
        });
    }
}
