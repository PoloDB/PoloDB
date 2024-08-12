/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use serde::Serialize;
use bson::Document;
use std::borrow::Borrow;
use std::sync::Weak;
use serde::de::DeserializeOwned;
use crate::{ClientCursor, Error, IndexModel, Result};
use crate::db::db_inner::DatabaseInner;
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

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the size of all data in the collection.
    pub fn count_documents(&self) -> Result<u64> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        let count = db.count_documents(&self.name, &txn)?;
        Ok(count)
    }

    /// Updates up to one document matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    pub fn update_one(&self, query: Document, update: Document) -> Result<UpdateResult> {
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

    /// Updates all documents matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    pub fn update_many(&self, query: Document, update: Document) -> Result<UpdateResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        let result = try_db_op!(txn, db.update_many(&self.name, query, update, &txn));
        Ok(result)
    }

    /// Deletes up to one document found matching `query`.
    pub fn delete_one(&self, query: Document) -> Result<DeleteResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        let result = try_db_op!(txn, db.delete_one(&self.name, query, &txn));
        Ok(result)
    }

    /// When query is `None`, all the data in the collection will be deleted.
    ///
    /// The size of data deleted returns.
    pub fn delete_many(&self, query: Document) -> Result<DeleteResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        let result = try_db_op!(txn, db.delete_many(&self.name, query, &txn));
        Ok(result)
    }

    pub fn create_index(&self, index: IndexModel) -> Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        try_db_op!(txn, db.create_index(&self.name, index, &txn));
        Ok(())
    }

    /// Drops the index specified by `name` from this collection.
    pub fn drop_index(&self, name: impl AsRef<str>) -> Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        try_db_op!(txn, db.drop_index(&self.name, name.as_ref(), &txn));
        Ok(())
    }

    pub fn drop(&self) -> Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        try_db_op!(txn, db.drop_collection(&self.name, &txn));
        Ok(())
    }
}

impl<T>  Collection<T>
where
    T: Serialize,
{
    /// Inserts `doc` into the collection.
    pub fn insert_one(&self, doc: impl Borrow<T>) -> Result<InsertOneResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        let result = try_db_op!(txn, db.insert_one(
            &self.name,
            bson::to_document(doc.borrow())?,
            &txn,
        ));
        Ok(result)
    }

    /// Inserts the data in `docs` into the collection.
    pub fn insert_many(&self, docs: impl IntoIterator<Item = impl Borrow<T>>) -> Result<InsertManyResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        let result = try_db_op!(txn, db.insert_many(&self.name, docs, &txn));
        Ok(result)
    }
}

impl<T>  Collection<T>
    where
        T: DeserializeOwned,
{
    /// When query document is passed to the function. The result satisfies
    /// the query document.
    pub fn find(&self, filter: impl Into<Option<Document>>) -> Result<ClientCursor<T>> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        db.find_with_owned_session(&self.name, filter, txn)
    }

    /// Finds a single document in the collection matching `filter`.
    pub fn find_one(&self, filter: impl Into<Option<Document>>) -> Result<Option<T>> {
        let mut cursor = self.find(filter)?;
        let test = cursor.advance()?;
        if !test {
            return Ok(None);
        }
        return Ok(Some(cursor.deserialize_current()?));
    }

    /// Runs an aggregation operation.
    pub fn aggregate(&self, pipeline: impl IntoIterator<Item = Document>) -> Result<ClientCursor<T>> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let txn = db.start_transaction()?;
        db.aggregate_with_owned_session(&self.name, pipeline, txn)
    }

}
