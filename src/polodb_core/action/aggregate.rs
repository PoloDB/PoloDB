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
use crate::{ClientCursor, Error, Result};
use crate::db::db_inner::DatabaseInner;
use crate::transaction::TransactionInner;

pub struct Aggregate<'a, 'b, T: DeserializeOwned + Send + Sync = Document> {
    db: Weak<DatabaseInner>,
    name: &'a str,
    pipeline: Vec<Document>,
    txn: Option<&'b TransactionInner>,
    _phantom: std::marker::PhantomData<T>,
}

impl <'a, 'b , T: DeserializeOwned + Send + Sync> Aggregate<'a, 'b, T> {
    pub(crate) fn new(db: Weak<DatabaseInner>, name: &'a str, pipeline: Vec<Document>, txn: Option<&'b TransactionInner>) -> Aggregate<'a, 'b, T> {
        Aggregate {
            db,
            name,
            pipeline,
            txn,
            _phantom: Default::default(),
        }
    }

    pub fn run(self) -> Result<ClientCursor<T>> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = match self.txn {
            Some(txn) => txn.clone(),
            None => {
                db.start_transaction()?
            }
        };
        db.aggregate_with_owned_session(self.name, self.pipeline, txn.clone())
    }

    pub fn with_type<U>(self) -> Aggregate<'a, 'b, U>
    where U: DeserializeOwned + Send + Sync {
        Aggregate {
            db: self.db,
            name: self.name,
            pipeline: self.pipeline,
            txn: self.txn,
            _phantom: Default::default(),
        }
    }
}
