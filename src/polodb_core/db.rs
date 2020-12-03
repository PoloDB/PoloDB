use std::rc::Rc;
use std::path::Path;
use polodb_bson::{Document, ObjectId};
use super::error::DbErr;
use crate::Config;
use crate::context::DbContext;
use crate::{DbHandle, TransactionType};
use crate::dump::FullDump;

fn consume_handle_to_vec(handle: &mut DbHandle, result: &mut Vec<Rc<Document>>) -> DbResult<()> {
    handle.step()?;

    while handle.has_row() {
        let doc = handle.get().unwrap_document();
        result.push(doc.clone());

        handle.step()?;
    }

    Ok(())
}

/// A wrapper of collection in struct.
///
/// All CURD methods can be done through this structure.
///
/// Find/Update/Delete operations need a query object.
///
/// ## Query operation:
///
/// | Name | Description |
/// | ----------| ----------- |
/// | $eq | Matches values that are equal to a specified value. |
/// | $gt | Matches values that are greater than a specified value. |
/// | $gte | Matches values that are greater than or equal to a specified value. |
/// | $in | Matches any of the values specified in an array. |
/// | $lt | Matches values that are less than a specified value. |
/// | $lte | Matches values that are less than or equal to a specified value. |
/// | $ne | Matches all values that are not equal to a specified value. |
/// | $nin | Matches none of the values specified in an array. |
///
/// ## Logical operation:
///
/// | Name | Description |
/// | ---- | ----------- |
/// | $and | Joins query clauses with a logical AND returns all documents that match the conditions of both clauses. |
/// | $or | Joins query clauses with a logical OR returns all documents that match the conditions of either clause. |
///
/// ## Example:
///
/// ```rust
/// use std::rc::Rc;
/// use polodb_core::Database;
/// use polodb_bson::mk_document;
///
/// let mut db = Database::open("/tmp/test-collection").unwrap();
/// let mut collection = db.collection("test").unwrap();
/// collection.insert(Rc::new(mk_document! {
///     "_id": 0,
///     "name": "Vincent Chan",
///     "score": 99.99,
/// }));
/// ```
pub struct Collection<'a> {
    db: &'a mut Database,
    id: u32,
    meta_version: u32,
    name: String,
}

impl<'a>  Collection<'a> {

    fn new(db: &'a mut Database, id: u32, meta_version: u32, name: &str) -> Collection<'a> {
        Collection {
            db,
            id,
            meta_version,
            name: name.into(),
        }
    }

    /// When query is `None`, all the data in the collection return.
    ///
    /// When query document is passed to the function. The result satisfies
    /// the query document.
    pub fn find(&mut self, query: Option<&Document>) -> DbResult<Vec<Rc<Document>>> {
        let mut handle = self.db.ctx.find(self.id, self.meta_version, query)?;

        let mut result = Vec::new();

        consume_handle_to_vec(&mut handle, &mut result)?;

        Ok(result)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the size of all data in the collection.
    #[inline]
    pub fn count(&mut self) -> DbResult<u64> {
        self.db.ctx.count(self.id, self.meta_version)
    }

    /// When query is `None`, all the data in the collection will be updated.
    /// Basically the same as [MongoDB](https://docs.mongodb.com/manual/reference/operator/update-field/).
    ///
    /// ## Field Update Operators:
    ///
    /// | Name | Description |
    /// | ---- | ----------- |
    /// | $inc | Increments the value of the field by the specified amount. |
    /// | $min | Only updates the field if the specified value is less than the existing field value. |
    /// | $max | Only updates the field if the specified value is greater than the existing field value. |
    /// | $mul | Multiplies the value of the field by the specified amount. |
    /// | $rename | Renames a field. |
    /// | $set | Sets the value of a field in a document. |
    /// | $unset | Removes the specified field from a document. |
    #[inline]
    pub fn update(&mut self, query: Option<&Document>, update: &Document) -> DbResult<usize> {
        self.db.ctx.update(self.id, self.meta_version, query, update)
    }

    #[inline]
    pub fn insert(&mut self, doc: &mut Document) -> DbResult<()> {
        self.db.ctx.insert(self.id, self.meta_version, doc)
    }

    /// When query is `None`, all the data in the collection will be deleted.
    ///
    /// The size of data deleted returns.
    #[inline]
    pub fn delete(&mut self, query: Option<&Document>) -> DbResult<usize> {
        match query {
            Some(query) =>
                self.db.ctx.delete(self.id, self.meta_version, query),
            None =>
                self.db.ctx.delete_all(self.id, self.meta_version),
        }
    }

    // // release in 0.2
    #[allow(dead_code)]
    fn create_index(&mut self, keys: &Document, options: Option<&Document>) -> DbResult<()> {
        self.db.ctx.create_index(self.id, keys, options)
    }

}

///
/// API wrapper for Rust-level
///
/// [open]: #method.open
/// [create_collection]: #method.create_collection
/// [collection]: #method.collection
///
/// Use [open] API to open a database. A main database file will be
/// generated in the path user provided.
///
/// When you own an instance of a Database, the instance holds a file
/// descriptor of the database file. When the Database instance is dropped,
/// the handle of the file will be released.
///
/// # Collection
/// A [Collection](./struct.Collection.html) is a dataset of a kind of data.
/// You can use [create_collection] to create a data collection.
/// To obtain an exist collection, use [collection],
///
/// # Transaction
///
/// [start_transaction]: #method.start_transaction
///
/// You an manually start a transaction by [start_transaction] method.
/// If you don't start it manually, a transaction will be automatically started
/// in your every operation.
///
/// # Example
///
/// ```rust
/// use polodb_core::Database;
///
/// let mut db = Database::open("/tmp/test-polo.db").unwrap();
/// let test_collection = db.collection("test").unwrap();
/// ```
pub struct Database {
    ctx: Box<DbContext>,
}

pub type DbResult<T> = Result<T, DbErr>;

impl Database {

    #[inline]
    pub fn mk_object_id(&mut self) -> ObjectId {
        self.ctx.object_id_maker().mk_object_id()
    }

    pub fn open<P: AsRef<Path>>(path: P) -> DbResult<Database>  {
        Database::open_with_config(path, Config::default())
    }

    pub fn open_with_config<P: AsRef<Path>>(path: P, config: Config) -> DbResult<Database>  {
        let ctx = DbContext::new(path.as_ref(), config)?;
        let rc_ctx = Box::new(ctx);

        Ok(Database {
            ctx: rc_ctx,
        })
    }

    pub fn create_collection(&mut self, name: &str) -> DbResult<Collection> {
        let collection_meta = self.ctx.create_collection(name)?;
        Ok(Collection::new(self,
                           collection_meta.id,
                           collection_meta.meta_version,
                           name))
    }

    /// Return the version of package version in string.
    /// Defined in `Cargo.toml`.
    #[inline]
    pub fn get_version() -> String {
        DbContext::get_version()
    }

    ///
    /// [error]: ../enum.DbErr.html
    ///
    /// Return an exist collection. If the collection is not exists,
    /// a new collection will be created.
    ///
    pub fn collection(&mut self, col_name: &str) -> DbResult<Collection> {
        let info = match self.ctx.get_collection_meta_by_name(col_name) {
            Ok(meta) => meta,
            Err(DbErr::CollectionNotFound(_)) => self.ctx.create_collection(col_name)?,
            Err(err) => return Err(err),
        };
        Ok(Collection::new(self, info.id, info.meta_version, col_name))
    }

    #[inline]
    pub fn dump(&mut self) -> DbResult<FullDump> {
        self.ctx.dump()
    }

    /// Manually start a transaction. There are three types of transaction.
    ///
    /// - `None`: Auto transaction
    /// - `Some(Transaction::Write)`: Write transaction
    /// - `Some(Transaction::Read)`: Read transaction
    ///
    /// When you pass `None` to type parameter. The PoloDB will go into
    /// auto mode. The PoloDB will go into read mode firstly, once the users
    /// execute write operations(insert/update/delete), the DB will turn into
    /// write mode.
    #[inline]
    pub fn start_transaction(&mut self, ty: Option<TransactionType>) -> DbResult<()> {
        self.ctx.start_transaction(ty)
    }

    #[inline]
    pub fn commit(&mut self) -> DbResult<()> {
        self.ctx.commit()
    }

    #[inline]
    pub fn rollback(&mut self) -> DbResult<()> {
        self.ctx.rollback()
    }

    #[allow(dead_code)]
    pub(crate) fn query_all_meta(&mut self) -> DbResult<Vec<Rc<Document>>> {
        self.ctx.query_all_meta()
    }

}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::env;
    use polodb_bson::{Document, Value, mk_document};
    use crate::{Database, Config};
    use std::borrow::Borrow;

    static TEST_SIZE: usize = 1000;

    fn prepare_db_with_config(db_name: &str, config: Config) -> Database {
        let mut db_path = env::temp_dir();
        let mut journal_path = env::temp_dir();

        let db_filename = String::from(db_name) + ".db";
        let journal_filename = String::from(db_name) + ".db.journal";

        db_path.push(db_filename);
        journal_path.push(journal_filename);

        let _ = std::fs::remove_file(db_path.as_path());
        let _ = std::fs::remove_file(journal_path);

        Database::open_with_config(db_path.as_path().to_str().unwrap(), config).unwrap()
    }

    fn prepare_db(db_name: &str) -> Database {
        prepare_db_with_config(db_name, Config::default())
    }

    fn create_and_return_db_with_items(db_name: &str, size: usize) -> Database {
        let mut db = prepare_db(db_name);
        let mut collection = db.create_collection("test").unwrap();

        // let meta = db.query_all_meta().unwrap();

        for i in 0..size {
            let content = i.to_string();
            let mut new_doc = mk_document! {
                "content": content,
            };
            collection.insert(&mut new_doc).unwrap();
        }

        db
    }

    #[test]
    fn test_create_collection_and_find_all() {
        let mut db = create_and_return_db_with_items("test-collection", TEST_SIZE);

        let mut test_collection = db.collection("test").unwrap();
        let all = test_collection.find( None).unwrap();

        for doc in &all {
            println!("object: {}", doc);
        }

        assert_eq!(TEST_SIZE, all.len())
    }

    #[test]
    fn test_transaction_commit() {
        let mut db = prepare_db("test-transaction");
        db.start_transaction(None).unwrap();
        let mut collection = db.create_collection("test").unwrap();

        for i in 0..10{
            let content = i.to_string();
            let mut new_doc = mk_document! {
                    "_id": i,
                    "content": content,
                };
            collection.insert(&mut new_doc).unwrap();
        }
        db.commit().unwrap()
    }

    #[test]
    fn test_commit_after_commit() {
        let mut config = Config::default();
        config.journal_full_size = 1;
        let mut db = prepare_db_with_config("test-commit-2", config);
        db.start_transaction(None).unwrap();
        let mut collection = db.create_collection("test").unwrap();

        for i in 0..1000 {
            let content = i.to_string();
            let mut new_doc = mk_document! {
                "_id": i,
                "content": content,
            };
            collection.insert(&mut new_doc).unwrap();
        }
        db.commit().unwrap();

        db.start_transaction(None).unwrap();
        let mut collection2 = db.create_collection("test-2").unwrap();
        for i in 0..10{
            let content = i.to_string();
            let mut new_doc = mk_document! {
                "_id": i,
                "content": content,
            };
            collection2.insert(&mut new_doc).expect(&*format!("insert failed: {}", i));
        }
        db.commit().unwrap();
    }

    #[test]
    fn test_rollback() {
        let mut db = prepare_db("test-rollback");
        let mut collection = db.create_collection("test").unwrap();

        assert_eq!(collection.count().unwrap(), 0);

        db.start_transaction(None).unwrap();

        let mut collection = db.collection("test").unwrap();
        for i in 0..10{
            let content = i.to_string();
            let mut new_doc = mk_document! {
                "_id": i,
                "content": content,
            };
            collection.insert(new_doc.as_mut()).unwrap();
        }
        assert_eq!(collection.count().unwrap(), 10);

        db.rollback().unwrap();

        let mut collection = db.collection("test").unwrap();
        assert_eq!(collection.count().unwrap(), 0);
    }

    #[test]
    fn test_create_collection_with_number_pkey() {
        let mut db = {
            let mut db = prepare_db("test-number-pkey");
            let mut collection = db.create_collection("test").unwrap();

            for i in 0..TEST_SIZE {
                let content = i.to_string();
                let mut new_doc = mk_document! {
                    "_id": i,
                    "content": content,
                };
                collection.insert(new_doc.as_mut()).unwrap();
            }

            db
        };

        let mut collection = db.collection("test").unwrap();

        let count = collection.count().unwrap();
        assert_eq!(TEST_SIZE, count as usize);

        let all = collection.find( None).unwrap();

        for doc in &all {
            println!("object: {}", doc);
        }

        assert_eq!(TEST_SIZE, all.len())
    }

    #[test]
    fn test_find() {
        let mut db = create_and_return_db_with_items("test-find", TEST_SIZE);
        let mut collection = db.collection("test").unwrap();

        let result = collection.find(
            Some(mk_document! {
                "content": "3",
            }.borrow())
        ).unwrap();

        assert_eq!(result.len(), 1);

        let one = result[0].clone();
        assert_eq!(one.get("content").unwrap().unwrap_string(), "3");
    }

    #[test]
    fn test_create_collection_and_find_by_pkey() {
        let mut db = create_and_return_db_with_items("test-find-pkey", 10);
        let mut collection = db.collection("test").unwrap();

        let all = collection.find(None).unwrap();

        assert_eq!(all.len(), 10);

        let first_key = &all[0].pkey_id().unwrap();

        let result = collection.find(Some(mk_document! {
            "_id": first_key.clone(),
        }.borrow())).unwrap();

        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_reopen_db() {
        {
            let _db1 = create_and_return_db_with_items("test-reopen", 5);
        }

        {
            let mut db_path = env::temp_dir();
            db_path.push("test-reopen.db");
            let _db2 = Database::open(db_path.as_path().to_str().unwrap()).unwrap();
        }
    }

    #[test]
    fn test_pkey_type_check() {
        let mut db = create_and_return_db_with_items("test-type-check", TEST_SIZE);

        let mut doc = mk_document! {
            "_id": 10,
            "value": "something",
        };

        let mut collection = db.collection("test").unwrap();
        collection.insert(doc.as_mut()).expect_err("should not success");
    }

    #[test]
    fn test_insert_bigger_key() {
        let mut db = prepare_db("test-insert-bigger-key");
        let mut collection = db.create_collection("test").unwrap();

        let mut doc = Document::new_without_id();

        let mut new_str: String = String::new();
        for _i in 0..32 {
            new_str.push('0');
        }

        doc.insert("_id".into(), Value::String(Rc::new(new_str.clone())));

        let _ = collection.insert(doc.as_mut()).unwrap();
    }

    #[test]
    fn test_create_index() {
        let mut db = prepare_db("test-create-index");
        let mut collection = db.create_collection("test").unwrap();

        let keys = mk_document! {
            "user_id": 1,
        };

        collection.create_index(&keys, None).unwrap();

        for i in 0..10 {
            let str = Rc::new(i.to_string());
            let mut data = mk_document! {
                "name": str.clone(),
                "user_id": str.clone(),
            };
            collection.insert(data.as_mut()).unwrap();
        }

        let mut data = mk_document! {
            "name": "what",
            "user_id": 3,
        };
        collection.insert(data.as_mut()).expect_err("not comparable");
    }

    #[test]
    fn test_one_delete_item() {
        let mut db = prepare_db("test-delete-item");
        let mut collection = db.create_collection("test").unwrap();

        let mut doc_collection  = vec![];

        for i in 0..100 {
            let content = i.to_string();

            let mut new_doc = mk_document! {
                "content": content,
            };

            collection.insert(new_doc.as_mut()).unwrap();
            doc_collection.push(new_doc);
        }

        let third = &doc_collection[3];
        let third_key = third.get("_id").unwrap();
        let delete_doc = mk_document! {
            "_id": third_key.clone(),
        };
        assert!(collection.delete(Some(&delete_doc)).unwrap() > 0);
        assert_eq!(collection.delete(Some(&delete_doc)).unwrap(), 0);
    }

    #[test]
    fn test_delete_all_items() {
        let mut db = prepare_db("test-delete-all-items");
        let mut collection = db.create_collection("test").unwrap();

        let mut doc_collection  = vec![];

        for i in 0..1000 {
            let content = i.to_string();
            let mut new_doc = mk_document! {
                "_id": i,
                "content": content,
            };
            collection.insert(new_doc.as_mut()).unwrap();
            doc_collection.push(new_doc);
        }

        for doc in &doc_collection {
            let key = doc.get("_id").unwrap();
            let deleted = collection.delete(Some(&mk_document!{
                "_id": key.clone(),
            })).unwrap();
            assert!(deleted > 0, "delete nothing with key: {}", key);
            let find_doc = mk_document! {
                "_id": key.clone(),
            };
            let result = collection.find(Some(&find_doc)).unwrap();
            assert_eq!(result.len(), 0, "item with key: {}", key);
        }
    }

}
