use std::sync::{Arc, Mutex};
use polodb_core::Transaction;
use anyhow::{anyhow, Result};

#[derive(Clone)]
pub(crate) struct ConnectionContext {
    inner: Arc<ConnectionWrapperInner>,
}

impl ConnectionContext {

    pub(crate) fn start_transaction(&self, txn: Transaction) -> Result<()> {
        let mut txn_guard = self.inner.txn.lock().unwrap();
        if txn_guard.is_some() {
            return Err(anyhow!("Transaction already exists"));
        }
        *txn_guard = Some(txn);
        Ok(())
    }

    pub(crate) fn commit_transaction(&self) -> Result<()> {
        let mut txn = self.inner.txn.lock().unwrap();
        if let Some(txn) = txn.take() {
            txn.commit()?;
        } else {
            return Err(anyhow!("No transaction to commit"));
        }
        Ok(())
    }

    pub(crate) fn abort_transaction(&self) -> Result<()> {
        let mut txn = self.inner.txn.lock().unwrap();
        if let Some(txn) = txn.take() {
            txn.rollback()?;
        } else {
            return Err(anyhow!("No transaction to abort"));
        }
        Ok(())
    }

    pub(crate) fn get_transaction(&self) -> Option<Transaction> {
        let txn = self.inner.txn.lock().unwrap();
        txn.clone()
    }

}

impl Default for ConnectionContext {

    fn default() -> Self {
        ConnectionContext {
            inner: Arc::new(ConnectionWrapperInner {
                txn: Mutex::new(None),
            }),
        }
    }

}

struct ConnectionWrapperInner {
    txn: Mutex<Option<Transaction>>,
}
