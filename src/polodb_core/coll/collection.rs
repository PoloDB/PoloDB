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

use serde::Serialize;
use bson::Document;
use std::borrow::Borrow;
use std::sync::Weak;
use serde::de::DeserializeOwned;
use crate::{ClientCursor, Error, IndexModel, Result};
use crate::db::db_inner::DatabaseInner;
use crate::find::Find;
use crate::results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};

macro_rules! try_multiple {
    ($err: expr, $action: expr) => {
        match $action {
            Ok(ret) => ret,
            Err(expr_err) => {
                return Err($err.add(expr_err))
            },
        }
    }
}

macro_rules! try_db_op {
    ($txn: expr, $action: expr) => {
        match $action {
            Ok(ret) => {
                $txn.commit()?;
                ret
            }

            Err(err) => {
                try_multiple!(err, $txn.rollback());
                return Err(err);
            }
        }
    }
}

pub trait CollectionT<T> {
    fn name(&self) -> &str;
    /// Return the size of all data in the collection.
    fn count_documents(&self) -> Result<u64>;

    /// Updates up to one document matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    fn update_one(&self, query: Document, update: Document) -> Result<UpdateResult>;

    /// Updates all documents matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    fn update_many(&self, query: Document, update: Document) -> Result<UpdateResult>;

    /// Deletes up to one document found matching `query`.
    fn delete_one(&self, query: Document) -> Result<DeleteResult>;

    /// When query is `None`, all the data in the collection will be deleted.
    ///
    /// The size of data deleted returns.
    fn delete_many(&self, query: Document) -> Result<DeleteResult>;
    fn create_index(&self, index: IndexModel) -> Result<()>;

    /// Drops the index specified by `name` from this collection.
    fn drop_index(&self, name: impl AsRef<str>) -> Result<()>;
    fn drop(&self) -> Result<()>;

    /// Inserts `doc` into the collection.
    fn insert_one(&self, doc: impl Borrow<T>) -> Result<InsertOneResult>
    where T: Serialize;

    /// Inserts the data in `docs` into the collection.
    fn insert_many(&self, docs: impl IntoIterator<Item = impl Borrow<T>>) -> Result<InsertManyResult>
    where T: Serialize;

    /// When query document is passed to the function. The result satisfies
    /// the query document.
    fn find(&self, filter: impl Into<Option<Document>>) -> Find<'_, '_, T>
    where T: DeserializeOwned + Send + Sync;

    /// Finds a single document in the collection matching `filter`.
    fn find_one(&self, filter: impl Into<Option<Document>>) -> Result<Option<T>>
    where T: DeserializeOwned + Send + Sync;

    /// Runs an aggregation operation.
    fn aggregate(&self, pipeline: impl IntoIterator<Item = Document>) -> Result<ClientCursor<T>>
    where T: DeserializeOwned + Send + Sync;
}


/// A wrapper of collection in struct.
///
/// All CURD methods can be done through this structure.
///
/// It can be used to perform collection-level operations such as CRUD operations.
pub struct Collection<T> {
    db: Weak<DatabaseInner>,
    name: String,
    _phantom: std::marker::PhantomData<T>,
}

impl<T>  Collection<T>
{
    pub(crate) fn new(db: Weak<DatabaseInner>, name: &str) -> Collection<T> {
        Collection {
            db,
            name: name.into(),
            _phantom: std::default::Default::default(),
        }
    }
}

impl<T> CollectionT<T> for Collection<T> {

    fn name(&self) -> &str {
        &self.name
    }

    fn count_documents(&self) -> Result<u64> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        let count = db.count_documents(&self.name, &txn)?;
        Ok(count)
    }

    fn update_one(&self, query: Document, update: Document) -> Result<UpdateResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        let result = try_db_op!(txn, db.update_one(
            &self.name,
            Some(&query),
            &update,
            &txn,
        ));
        Ok(result)
    }

    fn update_many(&self, query: Document, update: Document) -> Result<UpdateResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        let result = try_db_op!(txn, db.update_many(&self.name, query, update, &txn));
        Ok(result)
    }

    fn delete_one(&self, query: Document) -> Result<DeleteResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        let result = try_db_op!(txn, db.delete_one(&self.name, query, &txn));
        Ok(result)
    }

    fn delete_many(&self, query: Document) -> Result<DeleteResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        let result = try_db_op!(txn, db.delete_many(&self.name, query, &txn));
        Ok(result)
    }

    fn create_index(&self, index: IndexModel) -> Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        try_db_op!(txn, db.create_index(&self.name, index, &txn));
        Ok(())
    }

    fn drop_index(&self, name: impl AsRef<str>) -> Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        try_db_op!(txn, db.drop_index(&self.name, name.as_ref(), &txn));
        Ok(())
    }

    fn drop(&self) -> Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        try_db_op!(txn, db.drop_collection(&self.name, &txn));
        Ok(())
    }

    fn insert_one(&self, doc: impl Borrow<T>) -> Result<InsertOneResult>
    where T: Serialize {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        let result = try_db_op!(txn, db.insert_one(
            &self.name,
            bson::to_document(doc.borrow())?,
            &txn,
        ));
        Ok(result)
    }

    fn insert_many(&self, docs: impl IntoIterator<Item = impl Borrow<T>>) -> Result<InsertManyResult>
    where T: Serialize {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        let result = try_db_op!(txn, db.insert_many(&self.name, docs, &txn));
        Ok(result)
    }

    fn find(&self, filter: impl Into<Option<Document>>) -> Find<T>
    where T: DeserializeOwned + Send + Sync {
        Find::new(self.db.clone(), &self.name, None, filter.into())
    }

    fn find_one(&self, filter: impl Into<Option<Document>>) -> Result<Option<T>>
    where T: DeserializeOwned + Send + Sync {
        let mut cursor = self.find(filter).run()?;
        let test = cursor.advance()?;
        if !test {
            return Ok(None);
        }
        Ok(Some(cursor.deserialize_current()?))
    }

    fn aggregate(&self, pipeline: impl IntoIterator<Item = Document>) -> Result<ClientCursor<T>>
    where T: DeserializeOwned + Send + Sync {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        db.aggregate_with_owned_session(&self.name, pipeline, txn)
    }
}
