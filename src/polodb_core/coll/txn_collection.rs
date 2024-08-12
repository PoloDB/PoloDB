use std::borrow::Borrow;
use std::sync::Weak;
use bson::Document;
use serde::Serialize;
use crate::db::db_inner::DatabaseInner;
use serde::de::DeserializeOwned;
use crate::{ClientCursor, ClientSessionCursor, Error, IndexModel, Result};
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

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the size of all data in the collection.
    pub fn count_documents(&self) -> crate::Result<u64> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.count_documents(&self.name, &self.txn)
    }

    /// Updates up to one document matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    pub fn update_one(&self, query: Document, update: Document) -> crate::Result<UpdateResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let result = db.update_one(
            &self.name,
            Some(&query),
            &update,
            &self.txn,
        )?;
        Ok(result)
    }

    /// Updates all documents matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    pub fn update_many(&self, query: Document, update: Document) -> crate::Result<UpdateResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let result = db.update_many(&self.name, query, update, &self.txn)?;
        Ok(result)
    }

    /// Deletes up to one document found matching `query`.
    pub fn delete_one(&self, query: Document) -> crate::Result<DeleteResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let result = db.delete_one(&self.name, query, &self.txn)?;
        Ok(result)
    }

    /// When query is `None`, all the data in the collection will be deleted.
    ///
    /// The size of data deleted returns.
    pub fn delete_many(&self, query: Document) -> crate::Result<DeleteResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let result = db.delete_many(&self.name, query, &self.txn)?;
        Ok(result)
    }

    pub fn create_index(&self, index: IndexModel) -> crate::Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.create_index(&self.name, index, &self.txn)?;
        Ok(())
    }

    pub fn drop(&self) -> crate::Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.drop_collection(&self.name, &self.txn)?;
        Ok(())
    }
}

impl<T>  TransactionalCollection<T>
where
    T: Serialize,
{
    /// Inserts `doc` into the collection.
    pub fn insert_one(&self, doc: impl Borrow<T>) -> crate::Result<InsertOneResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let result = db.insert_one(
            &self.name,
            bson::to_document(doc.borrow())?,
            &self.txn,
        )?;
        Ok(result)
    }

    /// Inserts the data in `docs` into the collection.
    pub fn insert_many(&self, docs: impl IntoIterator<Item = impl Borrow<T>>) -> crate::Result<InsertManyResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let result = db.insert_many(&self.name, docs, &self.txn)?;
        Ok(result)
    }
}

impl<T>  TransactionalCollection<T>
where
    T: DeserializeOwned,
{
    /// When query document is passed to the function. The result satisfies
    /// the query document.
    pub fn find(&self, filter: impl Into<Option<Document>>) -> Result<ClientSessionCursor<T>> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.find_with_borrowed_session(&self.name, filter, &self.txn)
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
        db.aggregate_with_owned_session(&self.name, pipeline, self.txn.clone())
    }

}
