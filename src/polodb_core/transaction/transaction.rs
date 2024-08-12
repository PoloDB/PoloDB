use std::sync::{Arc, Weak};
use serde::Serialize;
use crate::{TransactionalCollection};
use crate::db::db_inner::DatabaseInner;
use super::transaction_inner::TransactionInner;

pub struct Transaction {
    db: Weak<DatabaseInner>,
    inner: Arc<TransactionInner>,
}

impl Transaction {

    pub(crate) fn new(db: Weak<DatabaseInner>, inner: TransactionInner) -> Transaction {
        Transaction {
            db,
            inner: Arc::new(inner),
        }
    }

    ///
    /// [error]: ../enum.DbErr.html
    ///
    /// Return an exist collection. If the collection is not exists,
    /// a new collection will be created.
    ///
    pub fn collection<T: Serialize>(&self, col_name: &str) -> TransactionalCollection<T> {
        TransactionalCollection::new(self.db.clone(), col_name, self.inner.as_ref().clone())
    }

    #[inline]
    pub fn commit(&self) -> crate::Result<()> {
        self.inner.commit()
    }

    #[inline]
    pub fn rollback(&self) -> crate::Result<()> {
        self.inner.rollback()
    }

}
