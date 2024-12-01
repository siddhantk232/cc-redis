use std::{net::SocketAddr, time::SystemTime};

use futures_util::SinkExt;
use tokio_stream::StreamExt;
use tokio_util::codec::Decoder;

mod cmd;
mod resp;
mod store;

use cmd::Cmd;
use resp::RedisValueRef;
use store::{Store, StoreRef, Val};

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let store = StoreRef::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (stream, conn) = listener.accept().await?;
        let store = store.clone();

        tokio::spawn(async move {
            let mut transport = resp::RespParser::default().framed(stream);

            while let Some(input) = transport.next().await {
                let input = match input {
                    Ok(i) => i,
                    Err(e) => {
                        panic!("{}", e);
                    }
                };
                assert!(matches!(input, RedisValueRef::Array(_)));

                let input = input
                    .to_vec()
                    .expect("redis-cli must send array of bulk strings");
                let cmds = cmd::parse_cmds(input).unwrap();

                for cmd in cmds {
                    let resp = if let Cmd::Exec = cmd {
                        if store.transaction_exists(&conn).await {
                            let (_, cmds) = match store.remove_transaction(&conn).await {
                                Some(v) => v,
                                None => unreachable!(
                                    "transaction must exist because we just checked for it"
                                ),
                            };

                            let mut res = Vec::with_capacity(cmds.len());

                            let mut st = store.inner.lock().unwrap();
                            for tx_cmd in cmds {
                                let resp = run_cmd(tx_cmd, &mut st, &conn);
                                res.push(resp);
                            }
                            drop(st);

                            RedisValueRef::Array(res)
                        } else {
                            RedisValueRef::Error("ERR EXEC without MULTI".into())
                        }
                    } else {
                        let mut st = store.inner.lock().unwrap();
                        let resp = run_cmd(cmd, &mut st, &conn);
                        drop(st);
                        resp
                    };

                    transport.send(resp).await.unwrap();
                }
            }
        });
    }
}

fn run_cmd(cmd: Cmd, store: &mut Store, conn: &SocketAddr) -> RedisValueRef {
    use Cmd::*;
    match cmd {
        Ping => RedisValueRef::String("PONG".into()),
        Echo(msg) => RedisValueRef::String(msg.into()),
        Get(ref key) => {
            if let Some(resp) = queue_if_transaction(cmd.clone(), &conn, store) {
                return resp;
            }

            let res = match store.read(key) {
                Some(entry) => {
                    if entry.eat.is_some()
                        && SystemTime::now() > entry.eat.expect("checked for none")
                    {
                        dbg!(store.remove_entry(key).unwrap());
                        RedisValueRef::NullBulkString
                    } else {
                        assert!(matches!(entry.val, RedisValueRef::String(_)));
                        entry.val.clone()
                    }
                }
                None => RedisValueRef::NullBulkString,
            };

            res
        }
        Set(ref key, ref val, ref exp) => {
            if let Some(resp) = queue_if_transaction(cmd.clone(), &conn, store) {
                return resp;
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
            let _ = store.insert(key.clone(), val);
            RedisValueRef::String("OK".into())
        }
        Incr(ref key) => {
            if let Some(resp) = queue_if_transaction(cmd.clone(), &conn, store) {
                return resp;
            }

            let entry = store.read(key).unwrap_or(Val {
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
                    store.update(key.clone(), res);

                    RedisValueRef::Int(new_val)
                }
                None => RedisValueRef::Error("ERR value is not an integer or out of range".into()),
            };

            res
        }
        Multi => {
            // TODO: do we support nested transactions?
            store.create_transaction(*conn);
            RedisValueRef::String("OK".into())
        }
        Discard => match store.remove_transaction(conn) {
            Some(_) => RedisValueRef::String("OK".into()),
            None => RedisValueRef::Error("ERR DISCARD without MULTI".into()),
        },
        Exec => {
            panic!(
                "Exec is handled outside of `run_cmd` to avoid creating async rescursive functions"
            );
        }
    }
}

/// Queues the command if a transaction was started previously on this connection
/// Return the +QUEUED response as [RedisValueRef::String]
fn queue_if_transaction(cmd: Cmd, conn: &SocketAddr, store: &mut Store) -> Option<RedisValueRef> {
    if !store.transaction_exists(conn) {
        return None;
    }

    store.append_cmd_to_transaction(conn, cmd);

    Some(RedisValueRef::String("QUEUED".into()))
}
