use std::sync::Arc;
use std::time::SystemTime;

use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use tokio_util::codec::{Encoder, FramedRead};

mod cmd;
mod resp;

// temporary solution for today
#[derive(Debug)]
struct Val {
    val: String,
    eat: Option<SystemTime>,
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let store = Arc::new(scc::HashMap::<String, Val>::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;
        let store = store.clone();

        tokio::spawn(async move {
            loop {
                let mut reader = FramedRead::new(&mut stream, resp::RespParser::default());
                let input = reader.next().await.unwrap().unwrap();
                assert!(matches!(input, resp::RedisValueRef::Array(_)));

                let input = input
                    .to_vec()
                    .expect("redis-cli must send array of bulk strings");
                let cmds = cmd::parse_cmds(input).unwrap();

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
                                    let entry = val.get();

                                    if entry.eat.is_some()
                                        && SystemTime::now() > entry.eat.expect("checked for none")
                                    {
                                        dbg!("removed: ", val.remove_entry().0);
                                        encoder
                                            .encode(
                                                resp::RedisValueRef::NullBulkString,
                                                &mut response,
                                            )
                                            .unwrap();
                                    } else {
                                        encoder
                                            .encode(
                                                resp::RedisValueRef::String(
                                                    entry.val.clone().into(),
                                                ),
                                                &mut response,
                                            )
                                            .unwrap();
                                    }
                                }
                                None => {
                                    encoder
                                        .encode(resp::RedisValueRef::NullBulkString, &mut response)
                                        .unwrap();
                                }
                            };

                            stream.write(&response).await.unwrap();
                        }
                        cmd::Cmd::Set(key, val, exp) => {
                            let val = Val {
                                val,
                                eat: exp.map(|x| match x {
                                    cmd::Expiry::Ex(x) => SystemTime::now()
                                        .checked_add(std::time::Duration::from_secs(x as u64))
                                        .unwrap(),
                                    cmd::Expiry::Px(x) => SystemTime::now()
                                        .checked_add(std::time::Duration::from_millis(x as u64))
                                        .unwrap(),
                                }),
                            };
                            let _ = store.insert_async(key, val).await;
                            stream.write(b"+OK\r\n").await.unwrap();
                        }
                    }
                }
            }
        });
    }
}
