use std::{
    hash::Hash,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use scc::Equivalent;

use crate::{cmd::Cmd, resp::RedisValueRef};

#[derive(Debug, Clone)]
pub struct Val {
    pub val: RedisValueRef,
    pub eat: Option<SystemTime>,
}

#[derive(Debug, Default)]
struct Store {
    pub db: scc::HashMap<String, Val>,
    pub transactions: scc::HashMap<SocketAddr, Vec<Cmd>>,
}

#[derive(Debug, Default, Clone)]
pub struct StoreRef {
    pub(self) inner: Arc<Mutex<Store>>,
}

impl StoreRef {
    pub fn new() -> StoreRef {
        StoreRef {
            inner: Arc::new(Mutex::new(Store::default())),
        }
    }

    /// Check if a transaction exists for the given [SocketAddr]
    /// A transaction is started with the `MULTI`
    pub async fn transaction_exists(&self, addr: &SocketAddr) -> bool {
        self.inner
            .lock()
            .unwrap()
            .transactions
            .read(addr, |_x, _y| {})
            .is_some()
    }

    /// Create an empty transaction for the given connection
    pub async fn create_transaction(&self, conn: SocketAddr) {
        let _ = self.inner.lock().unwrap().transactions.upsert(conn, vec![]);
    }

    pub async fn remove_transaction(&self, conn: &SocketAddr) -> Option<(SocketAddr, Vec<Cmd>)> {
        self.inner.lock().unwrap().transactions.remove(conn)
    }

    /// Add [Cmd] to the transaction for the given [SocketAddr]
    pub async fn append_cmd_to_transaction(&self, addr: &SocketAddr, cmd: Cmd) {
        let _ = self
            .inner
            .lock()
            .unwrap()
            .transactions
            .entry(*addr)
            .and_modify(|v| v.push(cmd));
    }

    #[allow(unused)]
    pub async fn run_transaction() -> Result<(), ()> {
        todo!()
    }

    /// read the value of `key` from the store
    pub async fn read<Q>(&self, key: &Q) -> Option<Val>
    where
        Q: Equivalent<String> + Hash,
    {
        self.inner.lock().unwrap().db.read(key, |_k, v| v.clone())
    }

    /// Remove the entry from the store returning it if it exists
    pub async fn remove_entry<Q>(&self, key: &Q) -> Option<(String, Val)>
    where
        Q: Equivalent<String> + Hash,
    {
        self.inner.lock().unwrap().db.remove(key)
    }

    /// Insert an entry in the store
    /// Returns an error along with the supplied key-value pair if the key exists.
    pub async fn insert(&self, key: String, val: Val) -> Result<(), (String, Val)> {
        self.inner.lock().unwrap().db.insert(key, val)
    }

    /// Update the value
    pub async fn update(&self, key: String, val: Val) {
        self.inner.lock().unwrap().db.upsert(key, val);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_transactions() {
        use crate::cmd::Cmd;
        use crate::resp::RedisValueRef;

        let store = StoreRef::new();
        let addr = "127.0.0.1:2020".parse().unwrap();

        assert_eq!(store.transaction_exists(&addr).await, false);

        let cmd1 = Cmd::Set("key".to_string(), RedisValueRef::String("val".into()), None);
        store.create_transaction(addr).await;
        store.append_cmd_to_transaction(&addr, cmd1.clone()).await;

        assert_eq!(store.transaction_exists(&addr).await, true);

        let cmd2 = Cmd::Incr("key".to_string());
        store.append_cmd_to_transaction(&addr, cmd2.clone()).await;

        store
            .inner
            .lock()
            .unwrap()
            .transactions
            .read_async(&addr, |_, cmds| {
                assert_eq!(cmds, &vec![cmd1, cmd2]);
            })
            .await;
    }
}
