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

use std::sync::Weak;
use bson::Document;
use serde::de::DeserializeOwned;
use crate::db::db_inner::DatabaseInner;
use crate::{ClientCursor, Error, Result};
use crate::transaction::TransactionInner;

pub struct Find<'a, 'b, T: DeserializeOwned + Send + Sync> {
    db: Weak<DatabaseInner>,
    name: &'a str,
    txn: Option<&'b TransactionInner>,
    filter: Option<Document>,
    skip: Option<u64>,
    limit: Option<u64>,
    sort: Option<Document>,
    _phantom: std::marker::PhantomData<T>,
}

impl <'a, 'b , T: DeserializeOwned + Send + Sync> Find<'a, 'b, T> {
    pub(crate) fn new(db: Weak<DatabaseInner>, name: &'a str, txn: Option<&'b TransactionInner>, filter: Option<Document>) -> Find<'a, 'b, T> {
        Find {
            db,
            name,
            txn,
            filter,
            skip: None,
            limit: None,
            sort: None,
            _phantom: Default::default(),
        }
    }

    pub fn skip(mut self, skip: u64) -> Self {
        self.skip = Some(skip);
        self
    }

    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn sort(mut self, sort: Document) -> Self {
        self.sort = Some(sort);
        self
    }

    pub fn run(self) -> Result<ClientCursor<T>> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        match self.txn {
            Some(txn) => {
                db.find_with_borrowed_session(&self.name, self.filter, txn)
            },
            None => {
                let txn = db.start_transaction()?;
                db.find_with_owned_session(&self.name, self.filter, txn)
            }
        }
    }
}
