/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use serde::Serialize;
use bson::Document;
use std::borrow::Borrow;
use std::sync::{Mutex, Weak};
use serde::de::DeserializeOwned;
use crate::{ClientCursor, ClientSession, ClientSessionCursor, DbErr, DbResult};
use crate::db::db_inner::DatabaseInner;
use crate::results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};

/// A wrapper of collection in struct.
///
/// All CURD methods can be done through this structure.
///
/// It can be used to perform collection-level operations such as CRUD operations.
pub struct Collection<T> {
    db: Weak<Mutex<DatabaseInner>>,
    name: String,
    _phantom: std::marker::PhantomData<T>,
}

impl<T>  Collection<T>
{

    pub(super) fn new(db: Weak<Mutex<DatabaseInner>>, name: &str) -> Collection<T> {
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
    pub fn count_documents(&self) -> DbResult<u64> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        let mut session = db.start_session()?;
        db.count_documents(&self.name, &mut session)
    }

    /// Return the size of all data in the collection.
    pub fn count_documents_with_session(&self, session: &mut ClientSession) -> DbResult<u64> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        db.count_documents(&self.name, &mut session.inner)
    }

    /// Updates up to one document matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    pub fn update_one(&self, query: Document, update: Document) -> DbResult<UpdateResult> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
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
    pub fn update_one_with_session(&self, query: Document, update: Document, session: &mut ClientSession) -> DbResult<UpdateResult> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        db.update_one(
            &self.name,
            Some(&query),
            &update,
            &mut session.inner,
        )
    }

    /// Updates all documents matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    pub fn update_many(&self, query: Document, update: Document) -> DbResult<UpdateResult> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        let mut session = db.start_session()?;
        db.update_many(&self.name, query, update, &mut session)
    }

    /// Updates all documents matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    pub fn update_many_with_session(&self, query: Document, update: Document, session: &mut ClientSession) -> DbResult<UpdateResult> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        db.update_many(&self.name, query, update, &mut session.inner)
    }

    /// Deletes up to one document found matching `query`.
    pub fn delete_one(&self, query: Document) -> DbResult<DeleteResult> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        let mut session = db.start_session()?;
        db.delete_one(&self.name, query, &mut session)
    }

    /// Deletes up to one document found matching `query`.
    pub fn delete_one_with_session(&self, query: Document, session: &mut ClientSession) -> DbResult<DeleteResult> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        db.delete_one(&self.name, query, &mut session.inner)
    }

    /// When query is `None`, all the data in the collection will be deleted.
    ///
    /// The size of data deleted returns.
    pub fn delete_many(&self, query: Document) -> DbResult<DeleteResult> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        let mut session = db.start_session()?;
        db.delete_many(&self.name, query, &mut session)
    }

    /// When query is `None`, all the data in the collection will be deleted.
    ///
    /// The size of data deleted returns.
    pub fn delete_many_with_session(&self, query: Document, session: &mut ClientSession) -> DbResult<DeleteResult> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        db.delete_many(&self.name, query, &mut session.inner)
    }

    /// release in 0.12
    #[allow(dead_code)]
    fn create_index(&self, _keys: &Document, _options: Option<&Document>) -> DbResult<()> {
        // let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        // let mut db = db_ref.lock()?;
        // let mut session = db.start_session()?;
        // db.create_index(self.name.insert())
        unimplemented!()
    }

    pub fn drop(&self) -> DbResult<()> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        let mut session = db.start_session()?;
        db.drop_collection(&self.name, &mut session)
    }

    pub fn drop_with_session(&self, session: &mut ClientSession) -> DbResult<()> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        db.drop_collection(&self.name, &mut session.inner)
    }
}

impl<T>  Collection<T>
where
    T: Serialize,
{
    /// Inserts `doc` into the collection.
    pub fn insert_one(&self, doc: impl Borrow<T>) -> DbResult<InsertOneResult> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        let mut session = db.start_session()?;
        db.insert_one(
            &self.name,
            bson::to_document(doc.borrow())?,
            &mut session,
        )
    }

    /// Inserts `doc` into the collection.
    pub fn insert_one_with_session(&self, doc: impl Borrow<T>, session: &mut ClientSession) -> DbResult<InsertOneResult> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        db.insert_one(
            &self.name,
            bson::to_document(doc.borrow())?,
            &mut session.inner,
        )
    }

    /// Inserts the data in `docs` into the collection.
    pub fn insert_many(&self, docs: impl IntoIterator<Item = impl Borrow<T>>) -> DbResult<InsertManyResult> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        let mut session = db.start_session()?;
        db.insert_many(&self.name, docs, &mut session)
    }

    /// Inserts the data in `docs` into the collection.
    pub fn insert_many_with_session(
        &self,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        session: &mut ClientSession
    ) -> DbResult<InsertManyResult> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        db.insert_many(&self.name, docs, &mut session.inner)
    }
}

impl<T>  Collection<T>
    where
        T: DeserializeOwned,
{
    /// When query document is passed to the function. The result satisfies
    /// the query document.
    pub fn find(&self, filter: impl Into<Option<Document>>) -> DbResult<ClientCursor<T>> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        let session = db.start_session()?;
        db.find_with_owned_session(&self.name, filter, session)
    }

    /// When query document is passed to the function. The result satisfies
    /// the query document.
    pub fn find_with_session(&self, filter: impl Into<Option<Document>>, session: &mut ClientSession) -> DbResult<ClientSessionCursor<T>> {
        let db_ref = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let mut db = db_ref.lock()?;
        db.find_with_borrowed_session(&self.name, filter, &mut session.inner)
    }

}

// #[cfg(test)]
// mod tests {
//     use bson::{Document, doc};
//     use crate::test_utils::prepare_db;
//
//     #[test]
//     fn test_create_index() {
//         let db = prepare_db("test-create-index").unwrap();
//         let collection = db.collection::<Document>("test");
//
//         let keys = doc! {
//             "user_id": 1,
//         };
//
//         collection.create_index(&keys, None).unwrap();
//
//         for i in 0..10 {
//             let str = i.to_string();
//             let data = doc! {
//                 "name": str.clone(),
//                 "user_id": str.clone(),
//             };
//             collection.insert_one(data).unwrap();
//         }
//
//         let data = doc! {
//             "name": "what",
//             "user_id": 3,
//         };
//         collection.insert_one(data).expect_err("not comparable");
//     }
// }
