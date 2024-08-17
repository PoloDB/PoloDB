use std::sync::{Arc, Mutex};
use polodb_core::Transaction;

#[derive(Clone)]
pub(crate) struct SessionContext {
    inner: Arc<SessionContextInner>,
}

impl SessionContext {

    pub(crate) fn new(txn: Transaction) -> SessionContext {
        SessionContext {
            inner: Arc::new(SessionContextInner {
                txn: Mutex::new(Some(txn)),
            }),
        }
    }

    pub(crate) fn get_transaction(&self) -> Option<Transaction> {
        let txn = self.inner.txn.lock().unwrap();
        txn.clone()
    }

    pub(crate) fn clear_transaction(&self) {
        let mut txn = self.inner.txn.lock().unwrap();
        *txn = None;
    }

}

impl Default for SessionContext {

    fn default() -> Self {
        SessionContext {
            inner: Arc::new(SessionContextInner {
                txn: Mutex::new(None),
            }),
        }
    }

}

struct SessionContextInner {
    txn: Mutex<Option<Transaction>>,
}
