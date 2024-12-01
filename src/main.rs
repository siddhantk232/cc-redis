use std::{net::SocketAddr, time::SystemTime};

use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use tokio_util::codec::{Encoder, FramedRead};

mod cmd;
mod resp;
mod store;
#[macro_use]
mod utils;

use cmd::Cmd;
use resp::RedisValueRef;
use store::{StoreRef, Val};

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let store = StoreRef::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut stream, conn) = listener.accept().await?;
        let store = store.clone();

        tokio::spawn(async move {
            loop {
                let mut reader = FramedRead::new(&mut stream, resp::RespParser);
                let Some(input) = reader.next().await else {
                    continue;
                };
                let input = input.unwrap();
                assert!(matches!(input, RedisValueRef::Array(_)));

                let input = input
                    .to_vec()
                    .expect("redis-cli must send array of bulk strings");
                let cmds = cmd::parse_cmds(input).unwrap();

                for cmd in cmds {
                    use Cmd::*;
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
                        Get(ref key) => {
                            if let Some(resp) =
                                queue_if_transaction(cmd.clone(), &conn, store.clone()).await
                            {
                                write_response!(stream, resp);
                                return;
                            }

                            let res = match store.read(key).await {
                                Some(entry) => {
                                    if entry.eat.is_some()
                                        && SystemTime::now() > entry.eat.expect("checked for none")
                                    {
                                        dbg!(store.remove_entry(key).await.unwrap());
                                        RedisValueRef::NullBulkString
                                    } else {
                                        assert!(matches!(entry.val, RedisValueRef::String(_)));
                                        entry.val.clone()
                                    }
                                }
                                None => RedisValueRef::NullBulkString,
                            };

                            write_response!(stream, res);
                        }
                        Set(ref key, ref val, ref exp) => {
                            if let Some(resp) =
                                queue_if_transaction(cmd.clone(), &conn, store.clone()).await
                            {
                                write_response!(stream, resp);
                                return;
                            }

                            let val = Val {
                                val: val.clone(),
                                eat: exp.clone().map(|x| match x {
                                    cmd::Expiry::Ex(x) => SystemTime::now()
                                        .checked_add(std::time::Duration::from_secs(x as u64))
                                        .unwrap(),
                                    cmd::Expiry::Px(x) => SystemTime::now()
                                        .checked_add(std::time::Duration::from_millis(x as u64))
                                        .unwrap(),
                                }),
                            };
                            let _ = store.insert(key.clone(), val).await;
                            stream.write(b"+OK\r\n").await.unwrap();
                        }
                        Incr(ref key) => {
                            if let Some(resp) =
                                queue_if_transaction(cmd.clone(), &conn, store.clone()).await
                            {
                                write_response!(stream, resp);
                                return;
                            }

                            let entry = store.read(key).await.unwrap_or(Val {
                                val: RedisValueRef::String("0".into()),
                                eat: None,
                            });

                            let new_val = entry.val.to_string_int().map(|v| v + 1);

                            let res = match new_val {
                                Some(new_val) => {
                                    let res = RedisValueRef::String(new_val.to_string().into());
                                    let res = Val {
                                        val: res,
                                        eat: entry.eat,
                                    };
                                    store.update(key, res).await;

                                    RedisValueRef::Int(new_val)
                                }
                                None => RedisValueRef::Error(
                                    "ERR value is not an integer or out of range".into(),
                                ),
                            };

                            write_response!(stream, res);
                        }
                        Multi => {
                            // TODO: do we support nested transactions?
                            store.create_transaction(conn).await;
                            stream.write(b"+OK\r\n").await.unwrap();
                        }
                        Exec => {
                            if store.transaction_exists(&conn).await {
                                // TODO: actually exec tsx cmds
                                let _k = store.remove_transaction(&conn).await;
                                write_response!(stream, RedisValueRef::Array(vec![]));
                            } else {
                                write_response!(
                                    stream,
                                    RedisValueRef::Error("ERR EXEC without MULTI".into())
                                );
                            }
                        }
                    }
                }
            }
        });
    }
}

/// Queues the command if a transaction was started previously on this connection
/// Return the +QUEUED response as [RedisValueRef::String]
async fn queue_if_transaction(
    cmd: Cmd,
    conn: &SocketAddr,
    store: StoreRef,
) -> Option<RedisValueRef> {
    if !store.transaction_exists(conn).await {
        return None;
    }

    store.append_cmd_to_transaction(conn, cmd).await;

    Some(RedisValueRef::String("QUEUED".into()))
}
