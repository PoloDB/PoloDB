use std::rc::Rc;
use std::borrow::Borrow;
use std::path::Path;
use polodb_bson::{Document, Value, ObjectIdMaker, mk_document};
use super::page::{header_page_wrapper, PageHandler};
use super::error::DbErr;
use crate::vm::{SubProgram, VM, VmState};
use crate::db::DbResult;
use crate::meta_doc_helper::{meta_doc_key, MetaDocEntry};
use crate::index_ctx::{IndexCtx, merge_options_into_default};
use crate::btree::*;
use crate::page::{RawPage, TransactionState};
use crate::db_handle::DbHandle;
use crate::journal::TransactionType;

macro_rules! try_db_op {
    ($self: tt, $action: expr) => {
        match $action {
            Ok(ret) => {
                $self.page_handler.auto_commit()?;
                ret
            }

            Err(err) => {
                $self.page_handler.auto_rollback()?;
                $self.reset_meta_version()?;
                return Err(err);
            }
        }
    }
}

#[inline]
fn index_already_exists(index_doc: &Document, key: &str) -> bool {
    index_doc.get(key).is_some()
}

/**
 * API for all platforms
 */
pub struct DbContext {
    page_handler:        Box<PageHandler>,
    obj_id_maker:        ObjectIdMaker,
    meta_version:        u32,

}

#[derive(Debug, Clone, Copy)]
pub struct MetaSource {
    pub meta_version: u32,
    pub meta_id_counter: u32,
    pub meta_pid: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct CollectionMeta {
    pub id: u32,
    pub meta_version: u32,
}

impl DbContext {

    pub fn new(path: &Path) -> DbResult<DbContext> {
        let page_size = 4096;

        let page_handler = PageHandler::new(path, page_size)?;

        let obj_id_maker = ObjectIdMaker::new();

        let mut ctx = DbContext {
            page_handler: Box::new(page_handler),
            // first_page,
            obj_id_maker,
            meta_version: 0,
        };

        let meta_source = ctx.get_meta_source()?;
        ctx.meta_version = meta_source.meta_version;

        Ok(ctx)
    }

    /**
     * when the database rollback,
     * the cached meta_version maybe rollback, so read it again
     */
    fn reset_meta_version(&mut self) -> DbResult<()> {
        let meta = self.get_meta_source()?;
        self.meta_version = meta.meta_version;
        Ok(())
    }

    fn check_meta_version(&self, actual_meta_version: u32) -> DbResult<()> {
        if self.meta_version != actual_meta_version {
            return Err(DbErr::MetaVersionMismatched(self.meta_version, actual_meta_version));
        }
        return Ok(())
    }

    pub fn get_collection_meta_by_name(&mut self, name: &str) -> DbResult<CollectionMeta> {
        self.page_handler.auto_start_transaction(TransactionType::Read)?;

        let result = try_db_op!(self, self.internal_get_collection_id_by_name(name));

        Ok(result)
    }

    fn internal_get_collection_id_by_name(&mut self, name: &str) -> DbResult<CollectionMeta> {
        let meta_src = self.get_meta_source()?;

        let collection_meta = MetaDocEntry::new(0, "<meta>".into(), meta_src.meta_pid);

        let query_doc = mk_document! {
            "name": name,
        };

        let meta_doc = mk_document!{};

        let subprogram = SubProgram::compile_query(&collection_meta, &meta_doc, &query_doc)?;

        let mut handle = self.make_handle(subprogram);
        handle.step()?;

        if handle.state() == (VmState::HasRow as i8) {
            let doc = handle.get().unwrap_document();
            let int_raw = doc.pkey_id().unwrap().unwrap_int();

            handle.commit_and_close_vm()?;
            return Ok(CollectionMeta {
                id: int_raw as u32,
                meta_version: self.meta_version
            });
        }

        Err(DbErr::CollectionNotFound(name.into()))
    }

    pub(crate) fn get_meta_source(&mut self) -> DbResult<MetaSource> {
        let head_page = self.page_handler.pipeline_read_page(0)?;
        let head_page_wrapper = header_page_wrapper::HeaderPageWrapper::from_raw_page(head_page);
        let meta_id_counter = head_page_wrapper.get_meta_id_counter();
        let meta_version = head_page_wrapper.get_meta_version();
        let meta_pid = head_page_wrapper.get_meta_page_id();
        Ok(MetaSource {
            meta_id_counter,
            meta_version,
            meta_pid,
        })
    }

    pub fn create_collection(&mut self, name: &str) -> DbResult<CollectionMeta> {
        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        let meta = try_db_op!(self, self.internal_create_collection(name));

        Ok(meta)
    }

    fn check_collection_exist(&mut self, name: &str, meta_src: &MetaSource) -> DbResult<bool> {
        let collection_meta = MetaDocEntry::new(0, "<meta>".into(), meta_src.meta_pid);

        let query_doc = mk_document! {
            "name": name,
        };

        let meta_doc = mk_document!{};

        let subprogram = SubProgram::compile_query(&collection_meta, &meta_doc, &query_doc)?;

        let mut handle = self.make_handle(subprogram);
        handle.set_rollback_on_drop(false);

        handle.step()?;

        if handle.state() == (VmState::HasRow as i8) {
            return Ok(true);
        }

        Ok(false)
    }

    fn internal_create_collection(&mut self, name: &str) -> DbResult<CollectionMeta> {
        if name.is_empty() {
            return Err(DbErr::IllegalCollectionName(name.into()));
        }
        let mut meta_source = self.get_meta_source()?;

        let exist = self.check_collection_exist(name, &meta_source)?;
        if exist {
            return Err(DbErr::CollectionAlreadyExits(name.into()));
        }

        let mut doc = Document::new_without_id();

        let collection_id = meta_source.meta_id_counter;
        doc.insert(meta_doc_key::ID.into(), collection_id.into());

        doc.insert(meta_doc_key::NAME.into(), name.into());

        let root_pid = self.page_handler.alloc_page_id()?;
        doc.insert(meta_doc_key::ROOT_PID.into(), Value::Int(root_pid as i64));

        doc.insert(meta_doc_key::FLAGS.into(), Value::Int(0));

        let mut btree_wrapper = BTreePageInsertWrapper::new(
            &mut self.page_handler, meta_source.meta_pid);

        let insert_result = btree_wrapper.insert_item(&doc, false)?;

        if let Some(backward_item) = insert_result.backward_item {
            let new_root_id = self.page_handler.alloc_page_id()?;

            let raw_page = backward_item.write_to_page(&mut self.page_handler,
                                                       new_root_id, meta_source.meta_pid)?;
            self.page_handler.pipeline_write_page(&raw_page)?;

            meta_source.meta_pid = new_root_id;
        }

        meta_source.meta_id_counter += 1;
        meta_source.meta_version += 1;

        self.update_meta_source(&meta_source)?;

        Ok(CollectionMeta {
            id: collection_id,
            meta_version: meta_source.meta_version,
        })
    }

    fn update_meta_source(&mut self, meta_source: &MetaSource) -> DbResult<()> {
        let head_page = self.page_handler.pipeline_read_page(0)?;
        let mut head_page_wrapper = header_page_wrapper::HeaderPageWrapper::from_raw_page(head_page);
        head_page_wrapper.set_meta_page_id(meta_source.meta_pid);
        head_page_wrapper.set_meta_id_counter(meta_source.meta_id_counter);
        head_page_wrapper.set_meta_version(meta_source.meta_version);
        self.meta_version = meta_source.meta_version;
        self.page_handler.pipeline_write_page(&head_page_wrapper.0)
    }

    #[inline]
    fn item_size(&self) -> u32 {
        (self.page_handler.page_size - HEADER_SIZE) / ITEM_SIZE
    }

    pub(crate) fn make_handle(&mut self, program: SubProgram) -> DbHandle {
        let vm = VM::new(&mut self.page_handler, Box::new(program));
        DbHandle::new(vm)
    }

    pub(crate) fn find_collection_root_pid_by_id(&mut self, parent_pid: u32, root_pid: u32, id: u32) -> DbResult<(MetaDocEntry, Rc<Document>)> {
        let raw_page = self.page_handler.pipeline_read_page(root_pid)?;
        let item_size = self.item_size();
        let btree_node = BTreeNode::from_raw(&raw_page, parent_pid, item_size, &mut self.page_handler)?;
        let key = Value::from(id);
        if btree_node.is_empty() {
            return Err(DbErr::CollectionIdNotFound(id));
        }
        let result = btree_node.search(&key)?;
        match result {
            SearchKeyResult::Node(node_index) => {
                let item = &btree_node.content[node_index];
                let doc = self.page_handler.get_doc_from_ticket(&item.data_ticket)?.unwrap();
                let entry = MetaDocEntry::from_doc(doc.borrow());
                Ok((entry, doc))
            }

            SearchKeyResult::Index(child_index) => {
                let next_pid = btree_node.indexes[child_index];
                if next_pid == 0 {
                    return Err(DbErr::CollectionIdNotFound(id));
                }

                self.find_collection_root_pid_by_id(root_pid, next_pid, id)
            }

        }
    }

    pub fn create_index(&mut self, col_id: u32, keys: &Document, options: Option<&Document>) -> DbResult<()> {
        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        try_db_op!(self, self.internal_create_index(col_id, keys, options));

        Ok(())
    }

    fn internal_create_index(&mut self, col_id: u32, keys: &Document, options: Option<&Document>) -> DbResult<()> {
        let meta_source = self.get_meta_source()?;
        let (_, mut meta_doc) = self.find_collection_root_pid_by_id(
            0, meta_source.meta_pid, col_id)?;
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

        let key_col = Value::from(col_id);

        let meta_source = self.get_meta_source()?;
        let inserted = self.update_by_root_pid(
            0, meta_source.meta_pid, &key_col, mut_meta_doc)?;
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
        let new_oid = self.obj_id_maker.mk_object_id();
        new_doc.insert(meta_doc_key::ID.into(), new_oid.into());
        doc
    }

    pub fn insert(&mut self, col_id: u32, meta_version: u32, doc: Rc<Document>) -> DbResult<Rc<Document>> {
        self.check_meta_version(meta_version)?;

        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(self, self.internal_insert(col_id, doc));

        Ok(result)
    }

    fn internal_insert(&mut self, col_id: u32, doc: Rc<Document>) -> DbResult<Rc<Document>> {
        let meta_source = self.get_meta_source()?;
        let doc_value = self.fix_doc(doc);

        let (mut collection_meta, mut meta_doc) = self.find_collection_root_pid_by_id(
            0, meta_source.meta_pid, col_id)?;
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
            let key = Value::from(col_id);
            let updated= self.update_by_root_pid(
                0, meta_source.meta_pid, &key, meta_doc_mut)?;
            if !updated {
                panic!("unexpected: update meta page failed")
            }
        }
        // update meta end

        Ok(doc_value)
    }

    /// query: None for findAll
    pub fn find(&mut self, col_id: u32, meta_version: u32, query: Option<&Document>) -> DbResult<DbHandle> {
        self.check_meta_version(meta_version)?;

        let meta_source = self.get_meta_source()?;
        let (collection_meta, meta_doc) = self.find_collection_root_pid_by_id(
            0, meta_source.meta_pid, col_id)?;

        let subprogram = match query {
            Some(query) => SubProgram::compile_query(&collection_meta, meta_doc.borrow(), query),
            None => SubProgram::compile_query_all(&collection_meta),
        }?;

        let handle = self.make_handle(subprogram);

        Ok(handle)
    }

    pub fn update(&mut self, col_id: u32, meta_version: u32, query: Option<&Document>, update: &Document) -> DbResult<usize> {
        self.check_meta_version(meta_version)?;

        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(self, self.internal_update(col_id, query, update));

        Ok(result)
    }

    fn internal_update(&mut self, col_id: u32, query: Option<&Document>, update: &Document) -> DbResult<usize> {
        let meta_source = self.get_meta_source()?;
        let (collection_meta, _meta_doc) = self.find_collection_root_pid_by_id(
            0, meta_source.meta_pid, col_id)?;

        let subprogram = SubProgram::compile_update(&collection_meta, query, update)?;

        let mut vm = VM::new(&mut self.page_handler, Box::new(subprogram));
        vm.execute()?;

        Ok(vm.r2 as usize)
    }

    pub fn drop(&mut self, col_id: u32, meta_version: u32) -> DbResult<()> {
        self.check_meta_version(meta_version)?;

        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        try_db_op!(self, self.internal_drop(col_id));

        Ok(())
    }

    fn internal_drop(&mut self, col_id: u32) -> DbResult<()> {
        let mut meta_source = self.get_meta_source()?;
        let (collection_meta, _meta_doc) = self.find_collection_root_pid_by_id(
            0, meta_source.meta_pid, col_id)?;
        delete_all_helper::delete_all(&mut self.page_handler, collection_meta)?;

        let mut btree_wrapper = BTreePageDeleteWrapper::new(
            &mut self.page_handler, meta_source.meta_pid);

        let pkey = Value::from(col_id);
        btree_wrapper.delete_item(&pkey)?;

        meta_source.meta_version += 1;
        self.update_meta_source(&meta_source)
    }

    pub fn delete(&mut self, col_id: u32, meta_version: u32, query: &Document) -> DbResult<usize> {
        let primary_keys = self.get_primary_keys_by_query(col_id, meta_version, Some(query))?;

        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(self, self.internal_delete(col_id, &primary_keys));

        Ok(result)
    }

    fn internal_delete(&mut self, col_id: u32, primary_keys: &Vec<Value>) -> DbResult<usize> {
        for pkey in primary_keys {
            let _ = self.internal_delete_by_pkey(col_id, pkey)?;
        }

        Ok(primary_keys.len())
    }

    pub fn delete_all(&mut self, col_id: u32, meta_version: u32) -> DbResult<usize> {
        let primary_keys = self.get_primary_keys_by_query(col_id, meta_version, None)?;

        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(self, self.internal_delete(col_id, &primary_keys));

        Ok(result)
    }

    fn get_primary_keys_by_query(&mut self, col_id: u32, meta_version: u32, query: Option<&Document>) -> DbResult<Vec<Value>> {
        let mut handle = self.find(col_id, meta_version, query)?;
        let mut buffer = vec![];

        handle.step()?;

        while handle.has_row() {
            let doc = handle.get().unwrap_document();
            let pkey = doc.pkey_id().unwrap();
            buffer.push(pkey);

            handle.step()?;
        }

        Ok(buffer)
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

    pub(crate) fn delete_by_pkey(&mut self, col_id: u32, key: &Value) -> DbResult<Option<Rc<Document>>> {
        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(self, self.internal_delete_by_pkey(col_id, key));

        Ok(result)
    }

    fn internal_delete_by_pkey(&mut self, col_id: u32, key: &Value) -> DbResult<Option<Rc<Document>>> {
        let meta_source = self.get_meta_source()?;
        let (collection_meta, meta_doc) = self.find_collection_root_pid_by_id(
            0, meta_source.meta_pid, col_id)?;

        let mut delete_wrapper = BTreePageDeleteWrapper::new(
            &mut self.page_handler,
            collection_meta.root_pid as u32
        );
        let result = delete_wrapper.delete_item(key)?;
        delete_wrapper.flush_pages()?;

        if let Some(deleted_item) = &result {
            let index_ctx_opt = IndexCtx::from_meta_doc(meta_doc.borrow());
            if let Some(index_ctx) = &index_ctx_opt {
                index_ctx.delete_index_by_content(deleted_item.borrow(), &mut self.page_handler)?;
            }

            return Ok(result)
        }

        Ok(None)
    }

    pub fn count(&mut self, col_id: u32, meta_version: u32) -> DbResult<u64> {
        self.check_meta_version(meta_version)?;
        let meta_source = self.get_meta_source()?;
        let (collection_meta, _meta_doc) = self.find_collection_root_pid_by_id(
            0, meta_source.meta_pid, col_id)?;
        counter_helper::count(&mut self.page_handler, collection_meta)
    }

    pub fn query_all_meta(&mut self) -> DbResult<Vec<Rc<Document>>> {
        // let meta_page_id = self.get_meta_page_id()?;
        //
        // let mut result = vec![];
        // let mut cursor = Cursor::new(&mut self.page_handler, meta_page_id)?;
        //
        // while cursor.has_next() {
        //     let ticket = cursor.peek().unwrap();
        //     let doc = cursor.get_doc_from_ticket(&ticket)?;
        //     result.push(doc);
        //
        //     let _ = cursor.next()?;
        // }
        //
        // Ok(result)
        unimplemented!()
    }

    pub fn start_transaction(&mut self, ty: Option<TransactionType>) -> DbResult<()> {
        match ty {
            Some(ty) => {
                self.page_handler.start_transaction(ty)?;
                self.page_handler.set_transaction_state(TransactionState::User);
            }

            None => {
                self.page_handler.start_transaction(TransactionType::Read)?;
                self.page_handler.set_transaction_state(TransactionState::UserAuto);
            }

        }
        Ok(())
    }

    pub fn commit(&mut self) -> DbResult<()> {
        self.page_handler.commit()?;
        self.page_handler.set_transaction_state(TransactionState::NoTrans);
        Ok(())
    }

    pub fn rollback(&mut self) -> DbResult<()> {
        self.page_handler.rollback()?;
        self.page_handler.set_transaction_state(TransactionState::NoTrans);
        Ok(())
    }

    #[inline]
    pub fn object_id_maker(&mut self) -> &mut ObjectIdMaker {
        &mut self.obj_id_maker
    }

    pub fn get_version() -> String {
        const VERSION: &'static str = env!("CARGO_PKG_VERSION");
        return VERSION.into();
    }

}

impl Drop for DbContext {

    fn drop(&mut self) {
        let path = self.page_handler.journal_file_path().to_path_buf();
        let checkpoint_result = self.page_handler.checkpoint_journal();  // ignored
        if let Ok(_) = checkpoint_result {
            let _ = std::fs::remove_file(path);  // ignore the result
        }
    }

}

