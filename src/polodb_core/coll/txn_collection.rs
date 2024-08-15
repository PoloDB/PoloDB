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

use std::borrow::Borrow;
use std::sync::Weak;
use bson::Document;
use serde::Serialize;
use crate::db::db_inner::DatabaseInner;
use serde::de::DeserializeOwned;
use crate::{ClientCursor, CollectionT, Error, IndexModel, Result};
use crate::results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};
use crate::transaction::TransactionInner;

pub struct TransactionalCollection<T> {
    db: Weak<DatabaseInner>,
    name: String,
    pub(crate) txn: TransactionInner,
    _phantom: std::marker::PhantomData<T>,
}

impl <T> TransactionalCollection<T>
{
    pub(crate) fn new(db: Weak<DatabaseInner>, name: &str, txn: TransactionInner) -> TransactionalCollection<T> {
        TransactionalCollection {
            db,
            name: name.into(),
            txn,
            _phantom: std::default::Default::default(),
        }
    }

}

impl<T> CollectionT<T> for TransactionalCollection<T> {

    fn name(&self) -> &str {
        &self.name
    }

    fn count_documents(&self) -> crate::Result<u64> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.count_documents(&self.name, &self.txn)
    }

    fn update_one(&self, query: Document, update: Document) -> crate::Result<UpdateResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let result = db.update_one(
            &self.name,
            Some(&query),
            &update,
            &self.txn,
        )?;
        Ok(result)
    }

    fn update_many(&self, query: Document, update: Document) -> crate::Result<UpdateResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let result = db.update_many(&self.name, query, update, &self.txn)?;
        Ok(result)
    }

    fn delete_one(&self, query: Document) -> crate::Result<DeleteResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let result = db.delete_one(&self.name, query, &self.txn)?;
        Ok(result)
    }

    fn delete_many(&self, query: Document) -> crate::Result<DeleteResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let result = db.delete_many(&self.name, query, &self.txn)?;
        Ok(result)
    }

    fn create_index(&self, index: IndexModel) -> crate::Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.create_index(&self.name, index, &self.txn)?;
        Ok(())
    }

    fn drop_index(&self, name: impl AsRef<str>) -> Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.drop_index(&self.name, name.as_ref(), &self.txn)?;
        Ok(())
    }

    fn drop(&self) -> crate::Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.drop_collection(&self.name, &self.txn)?;
        Ok(())
    }

    fn insert_one(&self, doc: impl Borrow<T>) -> crate::Result<InsertOneResult>
    where T: Serialize {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let result = db.insert_one(
            &self.name,
            bson::to_document(doc.borrow())?,
            &self.txn,
        )?;
        Ok(result)
    }

    fn insert_many(&self, docs: impl IntoIterator<Item = impl Borrow<T>>) -> crate::Result<InsertManyResult>
    where T: Serialize {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let result = db.insert_many(&self.name, docs, &self.txn)?;
        Ok(result)
    }

    fn find(&self, filter: impl Into<Option<Document>>) -> Result<ClientCursor<T>>
    where T: DeserializeOwned + Send + Sync {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.find_with_borrowed_session(&self.name, filter, &self.txn)
    }

    fn find_one(&self, filter: impl Into<Option<Document>>) -> Result<Option<T>>
    where T: DeserializeOwned + Send + Sync {
        let mut cursor = self.find(filter)?;
        let test = cursor.advance()?;
        if !test {
            return Ok(None);
        }
        Ok(Some(cursor.deserialize_current()?))
    }

    fn aggregate(&self, pipeline: impl IntoIterator<Item = Document>) -> Result<ClientCursor<T>>
    where T: DeserializeOwned + Send + Sync {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.aggregate_with_owned_session(&self.name, pipeline, self.txn.clone())
    }
}
