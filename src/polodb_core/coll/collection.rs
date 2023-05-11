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
use crate::{ClientCursor, ClientSession, ClientSessionCursor, Error, IndexModel, Result};
use crate::db::db_inner::DatabaseInner;
use crate::results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};

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
        let mut session = db.start_session()?;
        db.count_documents(&self.name, &mut session)
    }

    /// Return the size of all data in the collection.
    pub fn count_documents_with_session(&self, session: &mut ClientSession) -> Result<u64> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.count_documents(&self.name, &mut session.inner)
    }

    /// Updates up to one document matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    pub fn update_one(&self, query: Document, update: Document) -> Result<UpdateResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let mut session = db.start_session()?;
        db.update_one(
            &self.name,
            Some(&query),
            &update,
            &mut session,
        )
    }

    /// Updates up to one document matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    pub fn update_one_with_session(&self, query: Document, update: Document, session: &mut ClientSession) -> Result<UpdateResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.update_one(
            &self.name,
            Some(&query),
            &update,
            &mut session.inner,
        )
    }

    /// Updates all documents matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    pub fn update_many(&self, query: Document, update: Document) -> Result<UpdateResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let mut session = db.start_session()?;
        db.update_many(&self.name, query, update, &mut session)
    }

    /// Updates all documents matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    pub fn update_many_with_session(&self, query: Document, update: Document, session: &mut ClientSession) -> Result<UpdateResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.update_many(&self.name, query, update, &mut session.inner)
    }

    /// Deletes up to one document found matching `query`.
    pub fn delete_one(&self, query: Document) -> Result<DeleteResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let mut session = db.start_session()?;
        db.delete_one(&self.name, query, &mut session)
    }

    /// Deletes up to one document found matching `query`.
    pub fn delete_one_with_session(&self, query: Document, session: &mut ClientSession) -> Result<DeleteResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.delete_one(&self.name, query, &mut session.inner)
    }

    /// When query is `None`, all the data in the collection will be deleted.
    ///
    /// The size of data deleted returns.
    pub fn delete_many(&self, query: Document) -> Result<DeleteResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let mut session = db.start_session()?;
        db.delete_many(&self.name, query, &mut session)
    }

    /// When query is `None`, all the data in the collection will be deleted.
    ///
    /// The size of data deleted returns.
    pub fn delete_many_with_session(
        &self,
        query: Document,
        session: &mut ClientSession,
    ) -> Result<DeleteResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.delete_many(&self.name, query, &mut session.inner)
    }

    pub fn create_index(&self, index: IndexModel) -> Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let mut session = db.start_session()?;
        db.create_index(&self.name, index, &mut session)
    }

    pub fn create_index_with_session(&self, index: IndexModel, session: &mut ClientSession) -> Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.create_index(&self.name, index, &mut session.inner)
    }

    /// Drops the index specified by `name` from this collection.
    pub fn drop_index(&self, name: impl AsRef<str>) -> Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let mut session = db.start_session()?;
        db.drop_index(&self.name, name.as_ref(), &mut session)
    }

    pub fn drop_index_with_session(&self, name: impl AsRef<str>, session: &mut ClientSession) -> Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.drop_index(&self.name, name.as_ref(), &mut session.inner)
    }

    pub fn drop(&self) -> Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let mut session = db.start_session()?;
        db.drop_collection(&self.name, &mut session)
    }

    pub fn drop_with_session(&self, session: &mut ClientSession) -> Result<()> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.drop_collection(&self.name, &mut session.inner)
    }
}

impl<T>  Collection<T>
where
    T: Serialize,
{
    /// Inserts `doc` into the collection.
    pub fn insert_one(&self, doc: impl Borrow<T>) -> Result<InsertOneResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let mut session = db.start_session()?;
        db.insert_one(
            &self.name,
            bson::to_document(doc.borrow())?,
            &mut session,
        )
    }

    /// Inserts `doc` into the collection.
    pub fn insert_one_with_session(&self, doc: impl Borrow<T>, session: &mut ClientSession) -> Result<InsertOneResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.insert_one(
            &self.name,
            bson::to_document(doc.borrow())?,
            &mut session.inner,
        )
    }

    /// Inserts the data in `docs` into the collection.
    pub fn insert_many(&self, docs: impl IntoIterator<Item = impl Borrow<T>>) -> Result<InsertManyResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        let mut session = db.start_session()?;
        db.insert_many(&self.name, docs, &mut session)
    }

    /// Inserts the data in `docs` into the collection.
    pub fn insert_many_with_session(
        &self,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        session: &mut ClientSession
    ) -> Result<InsertManyResult> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.insert_many(&self.name, docs, &mut session.inner)
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
        let session = db.start_session()?;
        db.find_with_owned_session(&self.name, filter, session)
    }

    /// When query document is passed to the function. The result satisfies
    /// the query document.
    pub fn find_with_session(&self, filter: impl Into<Option<Document>>, session: &mut ClientSession) -> Result<ClientSessionCursor<T>> {
        let db = self.db.upgrade().ok_or(Error::DbIsClosed)?;
        db.find_with_borrowed_session(&self.name, filter, &mut session.inner)
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

    /// Finds a single document in the collection matching `filter` using the provided
    /// `ClientSession`.
    pub fn find_one_with_session(
        &self,
        filter: impl Into<Option<Document>>,
        session: &mut ClientSession,
    ) -> Result<Option<T>> {
        let mut cursor = self.find_with_session(filter, session)?;
        let test = cursor.advance(session)?;
        if !test {
            return Ok(None);
        }
        return Ok(Some(cursor.deserialize_current()?));
    }

}
