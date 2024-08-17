// Copyright 2024 Vincent Chan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::{Arc, Weak};
use serde::Serialize;
use crate::{TransactionalCollection};
use crate::db::db_inner::DatabaseInner;
use super::transaction_inner::TransactionInner;

#[derive(Clone)]
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
