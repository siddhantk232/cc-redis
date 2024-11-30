use std::sync::Arc;
use std::time::SystemTime;

use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use tokio_util::codec::{Encoder, FramedRead};

mod cmd;
mod resp;
#[macro_use]
mod utils;

// temporary solution for today
#[derive(Debug)]
struct Val {
    val: resp::RedisValueRef,
    eat: Option<SystemTime>,
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    use cmd::Cmd::*;
    let store = Arc::new(scc::HashMap::<String, Val>::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;
        let store = store.clone();

        tokio::spawn(async move {
            loop {
                let mut reader = FramedRead::new(&mut stream, resp::RespParser);
                let input = reader.next().await.unwrap().unwrap();
                assert!(matches!(input, resp::RedisValueRef::Array(_)));

                let input = input
                    .to_vec()
                    .expect("redis-cli must send array of bulk strings");
                let cmds = cmd::parse_cmds(input).unwrap();

                for cmd in cmds {
                    match cmd {
                        Ping => {
                            stream.write(b"+PONG\r\n").await.unwrap();
                        }
                        Echo(msg) => {
                            stream
                                .write(format!("+{}\r\n", msg).as_bytes())
                                .await
                                .unwrap();
                        }
                        Get(key) => {
                            let res = match store.get_async(&key).await {
                                Some(val) => {
                                    let entry = val.get();

                                    if entry.eat.is_some()
                                        && SystemTime::now() > entry.eat.expect("checked for none")
                                    {
                                        dbg!("removed: ", val.remove_entry().0);
                                        resp::RedisValueRef::NullBulkString
                                    } else {
                                        assert!(matches!(
                                            entry.val,
                                            resp::RedisValueRef::String(_)
                                        ));
                                        entry.val.clone()
                                    }
                                }
                                None => resp::RedisValueRef::NullBulkString,
                            };

                            write_response!(stream, res);
                        }
                        Set(key, val, exp) => {
                            dbg!(&val);
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
                        Incr(key) => {
                            let mut entry = store.entry_async(key).await.or_insert(Val {
                                val: resp::RedisValueRef::String("0".into()),
                                eat: None,
                            });

                            let val = entry.get().val.clone();
                            let new_val = val.to_string_int().map(|v| v + 1);

                            let res = match new_val {
                                Some(new_val) => {
                                    let res =
                                        resp::RedisValueRef::String(new_val.to_string().into());
                                    entry.get_mut().val = res.clone();

                                    resp::RedisValueRef::Int(new_val)
                                }
                                None => resp::RedisValueRef::Error(
                                    "ERR value is not an integer or out of range".into(),
                                ),
                            };

                            write_response!(stream, res);
                        },
                        Multi => {
                            stream.write(b"+OK\r\n").await.unwrap();
                        }
                    }
                }
            }
        });
    }
}
