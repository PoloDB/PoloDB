/*
 * Copyright (c) 2020 Vincent Chan
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation; either version 3, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */
use std::rc::Rc;
use super::error::DbErr;
use crate::bson::{Document, ObjectId, Value};
use crate::context::DbContext;

// #[derive(Clone)]
pub struct Database {
    ctx: Box<DbContext>,
}

pub type DbResult<T> = Result<T, DbErr>;

impl Database {

    #[inline]
    pub fn mk_object_id(&mut self) -> ObjectId {
        self.ctx.obj_id_maker.mk_object_id()
    }

    pub fn open(path: &str) -> DbResult<Database>  {
        let ctx = DbContext::new(path)?;
        let rc_ctx = Box::new(ctx);

        Ok(Database {
            ctx: rc_ctx,
        })
    }

    pub fn create_collection(&mut self, name: &str) -> DbResult<()> {
        self.ctx.start_transaction()?;
        self.ctx.create_collection(name)?;
        self.ctx.commit()
    }

    pub fn get_version() -> String {
        const VERSION: &'static str = env!("CARGO_PKG_VERSION");
        return VERSION.into();
    }

    #[inline]
    pub fn find(&mut self, col_name: &str, query: &Document) -> DbResult<()> {
        self.ctx.find(col_name, query)
    }

    #[inline]
    pub fn update(&mut self, col_name: &str, query: &Document, update: &Document) -> DbResult<()> {
        self.ctx.update(col_name, query, update)
    }

    pub fn insert(&mut self, col_name: &str, doc: Rc<Document>) -> DbResult<Rc<Document>> {
        self.ctx.start_transaction()?;
        let doc = self.ctx.insert(col_name, doc)?;
        self.ctx.commit()?;
        Ok(doc)
    }

    pub fn delete(&mut self, col_name: &str, key: &Value) -> DbResult<Option<Rc<Document>>> {
        self.ctx.start_transaction()?;
        let result = self.ctx.delete(col_name, key)?;
        self.ctx.commit()?;
        Ok(result)
    }

    pub fn create_index(&mut self, col_name: &str, keys: &Document, options: Option<&Document>) -> DbResult<()> {
        self.ctx.start_transaction()?;
        self.ctx.create_index(col_name, keys, options)?;
        self.ctx.commit()
    }

    #[allow(dead_code)]
    pub(crate) fn query_all_meta(&mut self) -> DbResult<Vec<Rc<Document>>> {
        self.ctx.query_all_meta()
    }

}

#[cfg(test)]
mod tests {
    use crate::Database;
    use std::rc::Rc;
    use crate::bson::{Document, Value, mk_str};

    static TEST_SIZE: usize = 1000;

    fn prepare_db() -> Database {
        let _ = std::fs::remove_file("/tmp/test.db");
        let _ = std::fs::remove_file("/tmp/test.db.journal");

        Database::open("/tmp/test.db").unwrap()
    }

    fn create_and_return_db_with_items(size: usize) -> Database {
        let mut db = prepare_db();
        let _result = db.create_collection("test").unwrap();

        // let meta = db.query_all_meta().unwrap();

        for i in 0..size {
            let content = i.to_string();
            let mut new_doc = Document::new_without_id();
            new_doc.insert("content".into(), mk_str(&content));
            db.insert("test", Rc::new(new_doc)).unwrap();
        }

        db
    }

    #[test]
    fn test_create_collection() {
        let mut db = create_and_return_db_with_items(TEST_SIZE);

        let mut test_col_cursor = db.ctx.get_collection_cursor("test").unwrap();
        let mut counter = 0;
        while test_col_cursor.has_next() {
            // let ticket = test_col_cursor.peek().unwrap();
            // let doc = test_col_cursor.get_doc_from_ticket(&ticket).unwrap();
            let doc = test_col_cursor.next().unwrap().unwrap();
            println!("object: {}", doc);
            counter += 1;
        }

        assert_eq!(TEST_SIZE, counter)
    }

    #[test]
    fn test_reopen_db() {
        {
            let db1 = create_and_return_db_with_items(5);
        }

        {
            let db2 = Database::open("/tmp/test.db").unwrap();
        }
    }

    #[test]
    fn test_pkey_type_check() {
        let mut db = create_and_return_db_with_items(TEST_SIZE);

        let mut doc = Document::new_without_id();
        doc.insert("_id".into(), Value::Int(10));
        doc.insert("value".into(), mk_str("something"));

        db.insert("test", Rc::new(doc)).expect_err("should not succuess");
    }

    #[test]
    fn test_insert_bigger_key() {
        let mut db = prepare_db();
        let _result = db.create_collection("test").unwrap();

        let mut doc = Document::new_without_id();

        let mut new_str: String = String::new();
        for _i in 0..32 {
            new_str.push('0');
        }

        doc.insert("_id".into(), Value::String(Rc::new(new_str.clone())));

        let _ = db.insert("test", Rc::new(doc)).unwrap();

        let mut cursor = db.ctx.get_collection_cursor("test").unwrap();

        let get_one = cursor.next().unwrap().unwrap();
        let get_one_id = get_one.get("_id").unwrap().unwrap_string();

        assert_eq!(get_one_id, new_str);
    }

    #[test]
    fn test_create_index() {
        let mut db = prepare_db();
        let _result = db.create_collection("test").unwrap();

        let mut keys = Document::new_without_id();
        keys.insert("user_id".into(), Value::Int(1));

        db.create_index("test", &keys, None).unwrap();

        for i in 0..10 {
            let mut data = Document::new_without_id();
            let str = Rc::new(i.to_string());
            data.insert("name".into(), Value::String(str.clone()));
            data.insert("user_id".into(), Value::String(str.clone()));
            db.insert("test", Rc::new(data)).unwrap();
        }

        let mut data = Document::new_without_id();
        // let str = Rc::new("ggg".into());
        data.insert("name".into(), Value::String(Rc::new("what".into())));
        data.insert("user_id".into(), Value::Int(3));
        db.insert("test", Rc::new(data)).expect_err("not comparable");
    }

    #[test]
    fn test_one_delete_item() {
        let mut db = prepare_db();
        let _ = db.create_collection("test").unwrap();

        let mut collection  = vec![];

        for i in 0..100 {
            let content = i.to_string();
            let mut new_doc = Document::new_without_id();
            new_doc.insert("content".into(), mk_str(&content));
            let ret_doc = db.insert("test", Rc::new(new_doc)).unwrap();
            collection.push(ret_doc);
        }

        let third = &collection[3];
        let third_key = third.get("_id").unwrap();
        assert!(db.delete("test", third_key).unwrap().is_some());
        assert!(db.delete("test", third_key).unwrap().is_none());
    }

    #[test]
    fn test_delete_all_item() {
        let mut db = prepare_db();
        let _ = db.create_collection("test").unwrap();

        let mut collection  = vec![];

        for i in 0..100 {
            let content = i.to_string();
            let mut new_doc = Document::new_without_id();
            new_doc.insert("content".into(), mk_str(&content));
            let ret_doc = db.insert("test", Rc::new(new_doc)).unwrap();
            collection.push(ret_doc);
        }

        for doc in &collection {
            let key = doc.get("_id").unwrap();
            db.delete("test", key).unwrap();
        }
    }

    #[test]
    fn print_value_size() {
        let size = std::mem::size_of::<crate::bson::Value>();
        assert_eq!(size, 16);
    }

}
