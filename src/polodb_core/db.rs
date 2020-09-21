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
use std::borrow::Borrow;
use super::error::DbErr;
use super::page::{header_page_wrapper, PageHandler};
use crate::index_ctx::{IndexCtx, merge_options_into_default};
use crate::meta_doc_helper::{meta_doc_key, MetaDocEntry};
use crate::bson::ObjectIdMaker;
use crate::bson::{ObjectId, Document, Value, mk_str};
use crate::btree::*;
use crate::cursor::Cursor;
use crate::page::RawPage;

#[inline]
fn index_already_exists(index_doc: &Document, key: &str) -> bool {
    index_doc.get(key).is_some()
}

// #[derive(Clone)]
pub struct Database {
    ctx: Box<DbContext>,
}

pub type DbResult<T> = Result<T, DbErr>;

pub(crate) struct DbContext {
    page_handler :        Box<PageHandler>,

    pub obj_id_maker: ObjectIdMaker,

}

impl DbContext {

    fn new(path: &str) -> DbResult<DbContext> {
        let page_size = 4096;

        let page_handler = PageHandler::new(path, page_size)?;

        let obj_id_maker = ObjectIdMaker::new();

        let ctx = DbContext {
            page_handler: Box::new(page_handler),

            // first_page,
            obj_id_maker,
        };
        Ok(ctx)
    }

    fn get_meta_page_id(&mut self) -> DbResult<u32> {
        let head_page = self.page_handler.pipeline_read_page(0)?;
        let head_page_wrapper = header_page_wrapper::HeaderPageWrapper::from_raw_page(head_page);
        let result = head_page_wrapper.get_meta_page_id();

        if result == 0 {  // unexpected
            return Err(DbErr::MetaPageIdError);
        }

        Ok(result)
    }

    pub fn create_collection(&mut self, name: &str) -> DbResult<ObjectId> {
        let oid = self.obj_id_maker.mk_object_id();
        let mut doc = Document::new_without_id();
        doc.insert(meta_doc_key::ID.into(), mk_str(name));

        let root_pid = self.page_handler.alloc_page_id()?;
        doc.insert(meta_doc_key::ROOT_PID.into(), Value::Int(root_pid as i64));

        doc.insert(meta_doc_key::FLAGS.into(), Value::Int(0));

        let meta_page_id: u32 = self.get_meta_page_id()?;

        let mut btree_wrapper = BTreePageInsertWrapper::new(&mut self.page_handler, meta_page_id);

        let insert_result = btree_wrapper.insert_item(&doc, false)?;

        match insert_result.backward_item {
            Some(backward_item) => {
                let new_root_id = self.page_handler.alloc_page_id()?;

                let raw_page = backward_item.write_to_page(&mut self.page_handler, new_root_id, meta_page_id)?;
                self.page_handler.pipeline_write_page(&raw_page)?;

                self.update_meta_page_id_of_db(new_root_id)?;

                Ok(oid)
            }

            None => Ok(oid)
        }
    }

    fn update_meta_page_id_of_db(&mut self, new_meta_page_root_pid: u32) -> DbResult<()> {
        let head_page = self.page_handler.pipeline_read_page(0)?;
        let mut head_page_wrapper = header_page_wrapper::HeaderPageWrapper::from_raw_page(head_page);
        head_page_wrapper.set_meta_page_id(new_meta_page_root_pid);
        self.page_handler.pipeline_write_page(&head_page_wrapper.0)
    }

    #[inline]
    fn item_size(&self) -> u32 {
        (self.page_handler.page_size - HEADER_SIZE) / ITEM_SIZE
    }

    fn find_collection_root_pid_by_name(&mut self, parent_pid: u32, root_pid: u32, col_name: &str) -> DbResult<(MetaDocEntry, Rc<Document>)> {
        let raw_page = self.page_handler.pipeline_read_page(root_pid)?;
        let item_size = self.item_size();
        let btree_node = BTreeNode::from_raw(&raw_page, parent_pid, item_size, &mut self.page_handler)?;
        let key = Value::String(Rc::new(col_name.to_string()));
        let result = btree_node.search(&key)?;
        match result {
            SearchKeyResult::Node(node_index) => {
                let item = &btree_node.content[node_index];
                let doc = self.page_handler.get_doc_from_ticket(&item.data_ticket)?;
                let entry = MetaDocEntry::from_doc(doc.borrow());
                Ok((entry, doc))
            }

            SearchKeyResult::Index(child_index) => {
                let next_pid = btree_node.indexes[child_index];
                if next_pid == 0 {
                    return Err(DbErr::CollectionNotFound(col_name.into()));
                }

                self.find_collection_root_pid_by_name(root_pid, next_pid, col_name)
            }

        }
    }

    pub fn create_index(&mut self, col_name: &str, keys: &Document, options: Option<&Document>) -> DbResult<()> {
        let meta_page_id = self.get_meta_page_id()?;
        let (_, mut meta_doc) = self.find_collection_root_pid_by_name(0, meta_page_id, col_name)?;
        let mut_meta_doc = Rc::get_mut(&mut meta_doc).unwrap();

        for (key_name, value_of_key) in keys.iter() {
            if let Value::Int(1) = value_of_key {
                // nothing
            } else {
                return Err(DbErr::InvalidOrderOfIndex(key_name.into()));
            }

            match mut_meta_doc.get(meta_doc_key::INDEXES) {
                Some(indexes_obj) => match indexes_obj {
                    Value::Document(index_doc) => {
                        if index_already_exists(index_doc.borrow(), key_name) {
                            return Err(DbErr::IndexAlreadyExists(key_name.into()));
                        }

                        unimplemented!()
                    }

                    _ => {
                        panic!("unexpected: indexes object is not a Document");
                    }

                },

                None => {
                    // create indexes
                    let mut doc = Document::new_without_id();

                    let root_pid = self.page_handler.alloc_page_id()?;
                    let options_doc = merge_options_into_default(root_pid, options)?;
                    doc.insert(key_name.into(), Value::Document(Rc::new(options_doc)));

                    mut_meta_doc.insert(meta_doc_key::INDEXES.into(), Value::Document(Rc::new(doc)));
                }

            }
        }

        let key_col = Value::String(Rc::new(col_name.into()));
        let inserted = self.update_by_root_pid(0, meta_page_id, &key_col, mut_meta_doc)?;
        if !inserted {
            panic!("update failed");
        }

        Ok(())
    }

    #[inline]
    fn fix_doc(&mut self, mut doc: Rc<Document>) -> Rc<Document> {
        if doc.get(meta_doc_key::ID).is_some() {
            return doc;
        }

        let new_doc = Rc::make_mut(&mut doc);
        new_doc.insert(meta_doc_key::ID.into(), Value::ObjectId(Rc::new(self.obj_id_maker.mk_object_id())));
        doc
    }

    fn insert(&mut self, col_name: &str, doc: Rc<Document>) -> DbResult<Rc<Document>> {
        let meta_page_id = self.get_meta_page_id()?;
        let doc_value = self.fix_doc(doc);

        let (mut collection_meta, mut meta_doc) = self.find_collection_root_pid_by_name(0, meta_page_id, col_name)?;
        let meta_doc_mut = Rc::get_mut(&mut meta_doc).unwrap();

        let mut is_pkey_check_skipped = false;
        collection_meta.check_pkey_ty(&doc_value, &mut is_pkey_check_skipped)?;

        let mut insert_wrapper = BTreePageInsertWrapper::new(
            &mut self.page_handler, collection_meta.root_pid as u32);
        let insert_result = insert_wrapper.insert_item(doc_value.borrow(), false)?;

        let mut is_meta_changed = false;

        if let Some(backward_item) = &insert_result.backward_item {
            self.handle_insert_backward_item(meta_doc_mut, collection_meta.root_pid as u32, backward_item)?;
            is_meta_changed = true;
        }

        // insert successfully
        if is_pkey_check_skipped {
            collection_meta.merge_pkey_ty_to_meta(meta_doc_mut, doc_value.borrow());
            is_meta_changed = true;
        }

        // insert index begin
        let mut index_ctx_opt = IndexCtx::from_meta_doc(meta_doc_mut);
        if let Some(index_ctx) = &mut index_ctx_opt {
            let mut is_ctx_changed = false;

            index_ctx.insert_index_by_content(
                doc_value.borrow(),
                &insert_result.data_ticket,
                &mut is_ctx_changed,
                &mut self.page_handler
            )?;

            if is_ctx_changed {
                index_ctx.merge_to_meta_doc(meta_doc_mut);
                is_meta_changed = true;
            }
        }
        // insert index end

        // update meta begin
        if is_meta_changed {
            let key = Value::String(Rc::new(col_name.to_string()));
            let updated= self.update_by_root_pid(0, meta_page_id, &key, meta_doc_mut)?;
            if !updated {
                panic!("unexpected: update meta page failed")
            }
        }
        // update meta end

        Ok(doc_value)
    }

    fn update_by_root_pid(&mut self, parent_pid: u32, root_pid: u32, key: &Value, doc: &Document) -> DbResult<bool> {
        let page = self.page_handler.pipeline_read_page(root_pid)?;
        let btree_node = BTreeNode::from_raw(&page, parent_pid, self.item_size(), &mut self.page_handler)?;

        let search_result = btree_node.search(key)?;
        match search_result {
            SearchKeyResult::Node(idx) => {
                self.page_handler.free_data_ticket(&btree_node.content[idx].data_ticket)?;

                let new_ticket = self.page_handler.store_doc(doc)?;
                let new_btree_node = btree_node.clone_with_content(idx, BTreeNodeDataItem {
                    key: key.clone(),
                    data_ticket: new_ticket,
                });

                let mut page = RawPage::new(btree_node.pid, self.page_handler.page_size);
                new_btree_node.to_raw(&mut page)?;

                self.page_handler.pipeline_write_page(&page)?;

                Ok(true)
            }

            SearchKeyResult::Index(idx) => {
                let next_pid = btree_node.indexes[idx];
                if next_pid == 0 {
                    return Ok(false);
                }

                self.update_by_root_pid(root_pid, next_pid, key, doc)
            }

        }
    }

    fn handle_insert_backward_item(&mut self,
                            meta_doc_mut: &mut Document,
                            left_pid: u32,
                            backward_item: &InsertBackwardItem) -> DbResult<()> {

        let new_root_id = self.page_handler.alloc_page_id()?;

        #[cfg(feature = "log")]
        eprintln!("handle backward item, left_pid: {}, new_root_id: {}, right_pid: {}", left_pid, new_root_id, backward_item.right_pid);

        let new_root_page = backward_item.write_to_page(&mut self.page_handler, new_root_id, left_pid)?;
        self.page_handler.pipeline_write_page(&new_root_page)?;

        meta_doc_mut.insert(meta_doc_key::ROOT_PID.into(), Value::Int(new_root_id as i64));

        Ok(())
    }

    fn delete(&mut self, col_name: &str, key: &Value) -> DbResult<Option<Rc<Document>>> {
        let meta_page_id = self.get_meta_page_id()?;
        let (collection_meta, meta_doc) = self.find_collection_root_pid_by_name(0, meta_page_id, col_name)?;

        let mut delete_wrapper = BTreePageDeleteWrapper::new(
            &mut self.page_handler,
            collection_meta.root_pid as u32
        );
        let result = delete_wrapper.delete_item(key)?;

        if let Some(deleted_item) = &result {
            let index_ctx_opt = IndexCtx::from_meta_doc(meta_doc.borrow());
            if let Some(index_ctx) = &index_ctx_opt {
                index_ctx.delete_index_by_content(deleted_item.borrow(), &mut self.page_handler)?;
            }

            return Ok(result)
        }

        Ok(None)
    }

    fn get_collection_cursor(&mut self, col_name: &str) -> DbResult<Cursor> {
        let root_page_id: u32 = {
            let meta_page_id = self.get_meta_page_id()?;
            let (meta_entry, _) = self.find_collection_root_pid_by_name(0, meta_page_id, col_name)?;
            meta_entry.root_pid
        };

        Ok(Cursor::new(&mut self.page_handler, root_page_id as u32)?)
    }

    pub fn query_all_meta(&mut self) -> DbResult<Vec<Rc<Document>>> {
        let meta_page_id = self.get_meta_page_id()?;

        let mut result = vec![];
        let mut cursor = Cursor::new(&mut self.page_handler, meta_page_id)?;

        while cursor.has_next() {
            let ticket = cursor.peek().unwrap();
            let doc = cursor.get_doc_from_ticket(&ticket)?;
            result.push(doc);

            let _ = cursor.next()?;
        }

        Ok(result)
    }

    #[inline]
    pub fn start_transaction(&mut self) -> DbResult<()> {
        self.page_handler.start_transaction()
    }

    #[inline]
    pub fn commit(&mut self) -> DbResult<()> {
        self.page_handler.commit()
    }

    #[inline]
    #[allow(dead_code)]
    pub fn rollback(&mut self) -> DbResult<()> {
        self.page_handler.rollback()
    }

}

impl Drop for DbContext {

    fn drop(&mut self) {
        let _ = self.page_handler.checkpoint_journal();  // ignored
    }

}

impl Database {

    pub fn open(path: &str) -> DbResult<Database>  {
        let ctx = DbContext::new(path)?;
        let rc_ctx = Box::new(ctx);

        Ok(Database {
            ctx: rc_ctx,
        })
    }

    pub fn create_collection(&mut self, name: &str) -> DbResult<ObjectId> {
        self.ctx.start_transaction()?;
        let oid = self.ctx.create_collection(name)?;
        self.ctx.commit()?;
        Ok(oid)
    }

    pub fn get_version() -> String {
        const VERSION: &'static str = env!("CARGO_PKG_VERSION");
        return VERSION.into();
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
