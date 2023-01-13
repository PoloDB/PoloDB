use serde::Serialize;
use bson::Document;
use std::borrow::Borrow;
use serde::de::DeserializeOwned;
use crate::{Database, DbResult};
use crate::results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};

/// A wrapper of collection in struct.
///
/// All CURD methods can be done through this structure.
///
/// It can be used to perform collection-level operations such as CRUD operations.
pub struct Collection<'a, T> {
    db: &'a Database,
    name: String,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T>  Collection<'a, T>
where
    T: Serialize,
{

    pub(super) fn new(db: &'a Database, name: &str) -> Collection<'a, T> {
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
        self.db.count_documents(&self.name)
    }

    /// Updates up to one document matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    pub fn update_one(&self, query: Document, update: Document) -> DbResult<UpdateResult> {
        self.db.update_one(&self.name, query, update)
    }

    /// Updates all documents matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    pub fn update_many(&self, query: Document, update: Document) -> DbResult<UpdateResult> {
        self.db.update_many(&self.name, query, update)
    }

    /// Inserts `doc` into the collection.
    pub fn insert_one(&self, doc: impl Borrow<T>) -> DbResult<InsertOneResult> {
        self.db.insert_one(&self.name, doc)
    }

    /// Inserts the data in `docs` into the collection.
    pub fn insert_many(&self, docs: impl IntoIterator<Item = impl Borrow<T>>) -> DbResult<InsertManyResult> {
        self.db.insert_many(&self.name, docs)
    }

    /// Deletes up to one document found matching `query`.
    pub fn delete_one(&self, query: Document) -> DbResult<DeleteResult> {
        self.db.delete_one(&self.name, query)
    }

    /// When query is `None`, all the data in the collection will be deleted.
    ///
    /// The size of data deleted returns.
    pub fn delete_many(&self, query: Document) -> DbResult<DeleteResult> {
        self.db.delete_many(&self.name, query)
    }

    // /// release in 0.12
    // #[allow(dead_code)]
    // fn create_index(&mut self, keys: &Document, options: Option<&Document>) -> DbResult<()> {
    //     let col_meta = self.db
    //         .get_collection_meta_by_name(&self.name, true)?
    //         .unwrap();
    //     self.db.ctx.create_index(col_meta.id, keys, options)
    // }

}

impl<'a, T>  Collection<'a, T>
    where
        T: DeserializeOwned,
{
    /// When query document is passed to the function. The result satisfies
    /// the query document.
    pub fn find_many(&mut self, filter: impl Into<Option<Document>>) -> DbResult<Vec<T>> {
        self.db.find_many(&self.name, filter)
    }

    /// Return the first element in the collection satisfies the query.
    pub fn find_one(&mut self, filter: impl Into<Option<Document>>) -> DbResult<Option<T>> {
        self.db.find_one(&self.name, filter)
    }

}
