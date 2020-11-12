use std::rc::Rc;
use std::path::Path;
use polodb_bson::{Document, ObjectId, Value};
use super::error::DbErr;
use crate::context::DbContext;
use crate::DbHandle;

fn consume_handle_to_vec(handle: &mut DbHandle, result: &mut Vec<Rc<Document>>) -> DbResult<()> {
    handle.step()?;

    while handle.has_row() {
        let doc = handle.get().unwrap_document();
        result.push(doc.clone());

        handle.step()?;
    }

    Ok(())
}

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

    pub fn find(&mut self, query: Option<&Document>) -> DbResult<Vec<Rc<Document>>> {
        let mut handle = self.db.ctx.find(self.id, query)?;

        let mut result = Vec::new();

        consume_handle_to_vec(&mut handle, &mut result)?;

        Ok(result)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn count(&mut self) -> DbResult<u64> {
        self.db.ctx.count(self.id)
    }

    #[inline]
    pub fn update(&mut self, query: Option<&Document>, update: &Document) -> DbResult<usize> {
        self.db.ctx.update(self.id, query, update)
    }

    #[inline]
    pub fn insert(&mut self, doc: Rc<Document>) -> DbResult<Rc<Document>> {
        self.db.ctx.insert(self.id, doc)
    }

    #[inline]
    pub fn delete(&mut self, key: &Value) -> DbResult<Option<Rc<Document>>> {
        self.db.ctx.delete_by_pkey(self.id, key)
    }

    // // release in 0.2
    #[inline]
    fn create_index(&mut self, keys: &Document, options: Option<&Document>) -> DbResult<()> {
        self.db.ctx.create_index(self.id, keys, options)
    }

}

/*
 * API wrapper for Rust-level
 */
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
        let ctx = DbContext::new(path.as_ref())?;
        let rc_ctx = Box::new(ctx);

        Ok(Database {
            ctx: rc_ctx,
        })
    }

    pub fn create_collection(&mut self, name: &str) -> DbResult<Collection> {
        let collection_id = self.ctx.create_collection(name)?;
        let meta_source = self.ctx.get_meta_source()?;
        Ok(Collection::new(self, collection_id, meta_source.meta_version, name))
    }

    #[inline]
    pub fn get_version() -> String {
        DbContext::get_version()
    }

    pub fn collection(&mut self, col_name: &str) -> DbResult<Collection> {
        let meta_source = self.ctx.get_meta_source()?;
        let collection_id = self.ctx.get_collection_id_by_name(col_name)?;
        Ok(Collection::new(self, collection_id, meta_source.meta_version, col_name))
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
    use crate::Database;
    use std::borrow::Borrow;

    static TEST_SIZE: usize = 1000;

    fn prepare_db(db_name: &str) -> Database {
        let mut db_path = env::temp_dir();
        let mut journal_path = env::temp_dir();

        let db_filename = String::from(db_name) + ".db";
        let journal_filename = String::from(db_name) + ".db.journal";

        db_path.push(db_filename);
        journal_path.push(journal_filename);

        let _ = std::fs::remove_file(db_path.as_path());
        let _ = std::fs::remove_file(journal_path);

        Database::open(db_path.as_path().to_str().unwrap()).unwrap()
    }

    fn create_and_return_db_with_items(db_name: &str, size: usize) -> Database {
        let mut db = prepare_db(db_name);
        let mut collection = db.create_collection("test").unwrap();

        // let meta = db.query_all_meta().unwrap();

        for i in 0..size {
            let content = i.to_string();
            let new_doc = mk_document! {
                "content": content,
            };
            collection.insert(Rc::new(new_doc)).unwrap();
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
    fn test_create_collection_with_number_pkey() {
        let mut db = {
            let mut db = prepare_db("test-number-pkey");
            let mut collection = db.create_collection("test").unwrap();

            for i in 0..TEST_SIZE {
                let content = i.to_string();
                let new_doc = mk_document! {
                    "_id": i,
                    "content": content,
                };
                collection.insert(Rc::new(new_doc)).unwrap();
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

        let doc = mk_document! {
            "_id": 10,
            "value": "something",
        };

        let mut collection = db.collection("test").unwrap();
        collection.insert(Rc::new(doc)).expect_err("should not success");
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

        let _ = collection.insert(Rc::new(doc)).unwrap();

        // let cursor = db.ctx.get_collection_cursor("test").unwrap();

        // let get_one = cursor.next().unwrap().unwrap();
        // let get_one_id = get_one.get("_id").unwrap().unwrap_string();

        // assert_eq!(get_one_id, new_str);
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
            let data = mk_document! {
                "name": str.clone(),
                "user_id": str.clone(),
            };
            collection.insert(Rc::new(data)).unwrap();
        }

        let data = mk_document! {
            "name": "what",
            "user_id": 3,
        };
        collection.insert(Rc::new(data)).expect_err("not comparable");
    }

    #[test]
    fn test_one_delete_item() {
        let mut db = prepare_db("test-delete-item");
        let mut collection = db.create_collection("test").unwrap();

        let mut doc_collection  = vec![];

        for i in 0..100 {
            let content = i.to_string();

            let new_doc = mk_document! {
                "content": content,
            };

            let ret_doc = collection.insert( Rc::new(new_doc)).unwrap();
            doc_collection.push(ret_doc);
        }

        let third = &doc_collection[3];
        let third_key = third.get("_id").unwrap();
        assert!(collection.delete(third_key).unwrap().is_some());
        assert!(collection.delete(third_key).unwrap().is_none());
    }

    #[test]
    fn test_delete_all_items() {
        let mut db = prepare_db("test-delete-all-items");
        let mut collection = db.create_collection("test").unwrap();

        let mut doc_collection  = vec![];

        for i in 0..1000 {
            let content = i.to_string();
            let new_doc = mk_document! {
                "_id": i,
                "content": content,
            };
            let ret_doc = collection.insert(Rc::new(new_doc)).unwrap();
            doc_collection.push(ret_doc);
        }

        for doc in &doc_collection {
            let key = doc.get("_id").unwrap();
            let deleted = collection.delete(key).unwrap();
            assert!(deleted.is_some(), "delete nothing with key: {}", key);
            let find_doc = mk_document! {
                "_id": key.clone(),
            };
            let result = collection.find(Some(&find_doc)).unwrap();
            assert_eq!(result.len(), 0, "item with key: {}", key);
        }
    }

}
