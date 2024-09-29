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
use bson::{Document, doc};
use serde::de::DeserializeOwned;
use crate::db::db_inner::DatabaseInner;
use crate::{ClientCursor, Error, Result};
use crate::transaction::TransactionInner;

pub struct Find<'a, 'b, T: DeserializeOwned + Send + Sync> {
    db: Weak<DatabaseInner>,
    name: &'a str,
    txn: Option<&'b TransactionInner>,
    filter: Document,
    skip: Option<u64>,
    limit: Option<u64>,
    sort: Option<Document>,
    _phantom: std::marker::PhantomData<T>,
}

impl <'a, 'b , T: DeserializeOwned + Send + Sync> Find<'a, 'b, T> {
    pub(crate) fn new(db: Weak<DatabaseInner>, name: &'a str, txn: Option<&'b TransactionInner>, filter: Document) -> Find<'a, 'b, T> {
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
        let txn = match self.txn {
            Some(txn) => txn.clone(),
            None => {
                db.start_transaction()?
            }
        };
        match (self.skip.as_ref(), self.limit.as_ref(), self.sort.as_ref()) {
            (None, None, None) => {
                db.find_with_owned_session(self.name, self.filter, txn)
            }
            _ => {
                let mut pipeline = vec![
                    doc! {
                        "$match": self.filter
                    }
                ];

                if let Some(sort) = self.sort {
                    pipeline.push(doc! {
                        "$sort": sort
                    });
                }

                if let Some(skip) = self.skip {
                    pipeline.push(doc! {
                        "$skip": skip as i64,
                    });
                }

                if let Some(limit) = self.limit {
                    pipeline.push(doc! {
                        "$limit": limit as i64,
                    });
                }

                db.aggregate_with_owned_session(self.name, pipeline, txn)
            }
        }
    }
}
