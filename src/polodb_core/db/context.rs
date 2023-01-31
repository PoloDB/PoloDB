use std::borrow::Borrow;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::rc::Rc;
use bson::{Document, Bson, doc, bson};
use serde::Serialize;
use super::db::DbResult;
use crate::page::header_page_wrapper;
use crate::error::DbErr;
use crate::{ClientSession, TransactionType};
use crate::page_handler::PageHandler;
use crate::Config;
use crate::vm::{SubProgram, VM, VmState};
use crate::meta_doc_helper::{meta_doc_key, MetaDocEntry};
use crate::index_ctx::{IndexCtx, merge_options_into_default};
use crate::btree::*;
use crate::transaction::TransactionState;
use crate::backend::memory::MemoryBackend;
use crate::page::RawPage;
use crate::db::db_handle::DbHandle;
use crate::dump::{FullDump, PageDump, OverflowDataPageDump, DataPageDump, FreeListPageDump, BTreePageDump};
use crate::page::header_page_wrapper::HeaderPageWrapper;
use crate::backend::Backend;
use crate::results::{InsertManyResult, InsertOneResult};
use crate::session::Session;
#[cfg(not(target_arch = "wasm32"))]
use crate::backend::file::FileBackend;
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;
use bson::oid::ObjectId;

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
    ($self: tt, $action: expr) => {
        match $action {
            Ok(ret) => {
                $self.page_handler.auto_commit()?;
                ret
            }

            Err(err) => {
                try_multiple!(err, $self.page_handler.auto_rollback());
                try_multiple!(err, $self.reset_meta_version());
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
pub(crate) struct DbContext {
    page_handler: Box<PageHandler>,
    pub(crate)meta_version: u32,
    _session_map: hashbrown::HashMap<ObjectId, Box<dyn Session + Send>>,
    #[allow(dead_code)]
    config:       Arc<Config>,
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

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_file(path: &Path, config: Config) -> DbResult<DbContext> {
        let page_size = NonZeroU32::new(4096).unwrap();

        let config = Arc::new(config);
        let backend = Box::new(FileBackend::open(path, page_size, config.clone())?);
        DbContext::open_with_backend(backend, page_size, config)
    }

    pub fn open_memory(config: Config) -> DbResult<DbContext> {
        let page_size = NonZeroU32::new(4096).unwrap();
        let config = Arc::new(config);
        let backend = Box::new(MemoryBackend::new(page_size, config.init_block_count));
        DbContext::open_with_backend(backend, page_size, config)
    }

    fn open_with_backend(backend: Box<dyn Backend + Send>, page_size: NonZeroU32, config: Arc<Config>) -> DbResult<DbContext> {
        let page_handler = PageHandler::new(backend, page_size, config.clone())?;

        let mut ctx = DbContext {
            page_handler: Box::new(page_handler),
            // first_page,
            meta_version: 0,
            _session_map: hashbrown::HashMap::new(),
            config,
        };

        let meta_source = ctx.get_meta_source()?;
        ctx.meta_version = meta_source.meta_version;

        Ok(ctx)
    }

    pub fn start_session(&mut self) -> DbResult<ClientSession> {
        let id = ObjectId::new();

        // TODO
        // let session = Box::new(DefaultSession::new(id));
        // self.session_map.insert(id, session);

        Ok(ClientSession::new(id))
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
        Ok(())
    }

    pub fn get_collection_meta_by_name(&mut self, name: &str) -> DbResult<CollectionMeta> {
        self.page_handler.auto_start_transaction(TransactionType::Read)?;

        let result = try_db_op!(self, self.internal_get_collection_id_by_name(name));

        Ok(result)
    }

    fn internal_get_collection_id_by_name(&mut self, name: &str) -> DbResult<CollectionMeta> {
        let meta_src = self.get_meta_source()?;

        let collection_meta = MetaDocEntry::new(0, "<meta>".into(), meta_src.meta_pid);

        let query_doc = doc! {
            "name": name,
        };

        let meta_doc = doc!{};

        let subprogram = SubProgram::compile_query(
            &collection_meta,
            &meta_doc,
            &query_doc,
            true)?;

        let mut handle = self.make_handle(subprogram);
        handle.step()?;

        if handle.state() == (VmState::HasRow as i8) {
            let doc = handle.get().as_document().unwrap();
            let int_raw = doc.get("_id").unwrap().as_i64().unwrap();

            handle.commit_and_close_vm()?;
            return Ok(CollectionMeta {
                id: int_raw as u32,
                meta_version: self.meta_version
            });
        }

        handle.commit_and_close_vm()?;
        Err(DbErr::CollectionNotFound(name.into()))
    }

    pub fn get_collection_meta_by_name_advanced_auto(&mut self, name: &str, create_if_not_exist: bool) -> DbResult<Option<CollectionMeta>> {
        self.page_handler.auto_start_transaction(if create_if_not_exist {
            TransactionType::Write
        } else {
            TransactionType::Read
        })?;

        let result = try_db_op!(self, self.get_collection_meta_by_name_advanced(name, create_if_not_exist));

        Ok(result)
    }

    pub fn get_collection_meta_by_name_advanced(&mut self, name: &str, create_if_not_exist: bool) -> DbResult<Option<CollectionMeta>> {
        match self.internal_get_collection_id_by_name(name) {
            Ok(meta) => Ok(Some(meta)),
            Err(DbErr::CollectionNotFound(_)) => {
                if create_if_not_exist {
                    let meta = self.internal_create_collection(name)?;
                    Ok(Some(meta))
                } else {
                    Ok(None)
                }
            },
            Err(err) => return Err(err),
        }
    }

    pub(crate) fn get_meta_source(&mut self) -> DbResult<MetaSource> {
        let head_page = self.page_handler.pipeline_read_page(0)?;
        DbContext::check_first_page_valid(&head_page)?;
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

    fn check_first_page_valid(page: &RawPage) -> DbResult<()> {
        let mut title_area: [u8; 32] = [0; 32];
        title_area.copy_from_slice(&page.data[0..32]);

        match std::str::from_utf8(&title_area) {
            Ok(s) => {
                if !s.starts_with("PoloDB") {
                    return Err(DbErr::NotAValidDatabase);
                }
                Ok(())
            },
            Err(_) => Err(DbErr::NotAValidDatabase),
        }
    }

    pub fn create_collection(&mut self, name: &str) -> DbResult<CollectionMeta> {
        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        let meta = try_db_op!(self, self.internal_create_collection(name));

        Ok(meta)
    }

    fn check_collection_exist(&mut self, name: &str, meta_src: &MetaSource) -> DbResult<bool> {
        let collection_meta = MetaDocEntry::new(0, "<meta>".into(), meta_src.meta_pid);

        let query_doc = doc! {
            "name": name,
        };

        let meta_doc = doc!{};

        let subprogram = SubProgram::compile_query(
            &collection_meta,
            &meta_doc,
            &query_doc,
            true)?;

        let mut handle = self.make_handle(subprogram);
        handle.set_rollback_on_drop(false);

        handle.step()?;

        let exist = handle.state() == (VmState::HasRow as i8);

        handle.commit_and_close_vm()?;

        Ok(exist)
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

        let mut doc = doc!();

        let collection_id = meta_source.meta_id_counter;
        doc.insert::<String, Bson>(meta_doc_key::ID.into(), Bson::Int64(collection_id as i64));

        doc.insert::<String, Bson>(meta_doc_key::NAME.into(), name.into());

        let root_pid = self.page_handler.alloc_page_id()?;
        doc.insert::<String, Bson>(meta_doc_key::ROOT_PID.into(), Bson::Int64(root_pid as i64));

        doc.insert::<String, Bson>(meta_doc_key::FLAGS.into(), bson!(0));

        let mut btree_wrapper = BTreePageInsertWrapper::new(
            self.page_handler.as_mut(), meta_source.meta_pid);

        let insert_result = btree_wrapper.insert_item(&doc, false)?;

        // if a backward item returns, it's saying that the btree has been "rotated".
        // the center node of the btree has been changed.
        // So you have to distribute a new page to store the "central node",
        // and the newer page is the center of the btree.
        if let Some(backward_item) = insert_result.backward_item {
            let new_root_id = self.page_handler.alloc_page_id()?;

            let raw_page = backward_item.write_to_page(self.page_handler.as_mut(),
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
        (self.page_handler.page_size.get() - HEADER_SIZE) / ITEM_SIZE
    }

    pub(crate) fn make_handle(&mut self, program: SubProgram) -> DbHandle {
        let vm = VM::new(self.page_handler.as_mut(), Box::new(program));
        DbHandle::new(vm)
    }

    pub(crate) fn find_collection_root_pid_by_id(&mut self, parent_pid: u32, root_pid: u32, id: u32) -> DbResult<MetaDocEntry> {
        let raw_page = self.page_handler.pipeline_read_page(root_pid)?;
        let item_size = self.item_size();
        let btree_node = BTreeNode::from_raw(&raw_page, parent_pid, item_size, self.page_handler.as_mut())?;
        let key = Bson::from(id);
        if btree_node.is_empty() {
            return Err(DbErr::CollectionIdNotFound(id));
        }
        let result = btree_node.search(&key)?;
        match result {
            SearchKeyResult::Node(node_index) => {
                let item = &btree_node.content[node_index];
                let doc = self.page_handler.get_doc_from_ticket(&item.data_ticket)?.unwrap();
                let entry = MetaDocEntry::from_doc(doc);
                Ok(entry)
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
        let mut meta_doc = self.find_collection_root_pid_by_id(
            0, meta_source.meta_pid, col_id)?;

        for (key_name, value_of_key) in keys.iter() {
            if let Bson::Int32(1) = value_of_key {
                // nothing
            } else if let Bson::Int64(1) = value_of_key {
                // nothing
            } else {
                return Err(DbErr::InvalidOrderOfIndex(key_name.clone()));
            }

            match meta_doc.doc_ref().get(meta_doc_key::INDEXES) {
                Some(indexes_obj) => match indexes_obj {
                    Bson::Document(index_doc) => {
                        if index_already_exists(index_doc.borrow(), key_name) {
                            return Err(DbErr::IndexAlreadyExists(key_name.clone()));
                        }

                        unimplemented!()
                    }

                    _ => {
                        panic!("unexpected: indexes object is not a Document");
                    }

                },

                None => {
                    // create indexes
                    let mut doc = doc!();

                    let root_pid = self.page_handler.alloc_page_id()?;
                    let options_doc = merge_options_into_default(root_pid, options)?;
                    doc.insert(key_name.clone(), Bson::Document(options_doc));

                    meta_doc.set_indexes(doc);
                }

            }
        }

        let key_col = Bson::from(col_id);

        let meta_source = self.get_meta_source()?;
        let inserted = self.update_by_root_pid(
            0, meta_source.meta_pid, &key_col, meta_doc.doc_ref())?;
        if !inserted {
            panic!("update failed");
        }

        Ok(())
    }

    #[inline]
    fn fix_doc(&mut self, doc: &mut Document) -> bool {
        if doc.get(meta_doc_key::ID).is_some() {
            return false;
        }

        let new_oid = bson::oid::ObjectId::new();
        doc.insert::<String, Bson>(meta_doc_key::ID.into(), new_oid.into());
        true
    }

    pub fn insert_one_auto(&mut self, col_name: &str, doc: &mut Document) -> DbResult<InsertOneResult> {
        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        let changed = try_db_op!(self, self.insert_one(col_name, doc));

        Ok(changed)
    }

    fn insert_one(&mut self, col_name: &str, doc: &mut Document) -> DbResult<InsertOneResult> {
        let col_meta = self.get_collection_meta_by_name_advanced_auto(col_name, true)?
            .expect("internal: meta must exist");
        self.insert_one_with_meta(&col_meta, doc)
    }

    fn insert_one_with_meta(&mut self, col_meta: &CollectionMeta, doc: &mut Document) -> DbResult<InsertOneResult> {
        self.check_meta_version(col_meta.meta_version)?;
        let col_id = col_meta.id;

        let meta_source = self.get_meta_source()?;
        let _changed  = self.fix_doc(doc);

        let mut collection_meta = self.find_collection_root_pid_by_id(
            0, meta_source.meta_pid, col_id)?;

        let pkey = doc.get("_id").unwrap();

        let mut is_pkey_check_skipped = false;
        collection_meta.check_pkey_ty(&pkey, &mut is_pkey_check_skipped)?;

        let mut is_meta_changed = false;

        // insert index begin
        let mut index_ctx_opt = IndexCtx::from_meta_doc(collection_meta.doc_ref());
        if let Some(index_ctx) = &mut index_ctx_opt {
            let mut is_ctx_changed = false;

            index_ctx.insert_index_by_content(
                doc,
                &pkey,
                &mut is_ctx_changed,
                &mut self.page_handler
            )?;

            if is_ctx_changed {
                index_ctx.merge_to_meta_doc(&mut collection_meta);
                is_meta_changed = true;
            }
        }
        // insert index end

        let mut insert_wrapper = BTreePageInsertWrapper::new(
            self.page_handler.as_mut(), collection_meta.root_pid());
        let insert_result: InsertResult = insert_wrapper.insert_item(doc, false)?;

        if let Some(backward_item) = &insert_result.backward_item {
            let root_pid = collection_meta.root_pid();
            self.handle_insert_backward_item(&mut collection_meta, root_pid, backward_item)?;
            is_meta_changed = true;
        }

        // insert successfully
        if is_pkey_check_skipped {
            collection_meta.merge_pkey_ty_to_meta(doc);
            is_meta_changed = true;
        }

        // update meta begin
        if is_meta_changed {
            let key = Bson::from(col_id);
            let updated= self.update_by_root_pid(
                0, meta_source.meta_pid, &key, collection_meta.doc_ref())?;
            if !updated {
                panic!("unexpected: update meta page failed")
            }
        }
        // update meta end

        Ok(InsertOneResult {
            inserted_id: pkey.clone(),
        })
    }

    pub fn insert_many_auto<T: Serialize>(&mut self, col_name: &str, docs: impl IntoIterator<Item = impl Borrow<T>>) -> DbResult<InsertManyResult> {
        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(self, self.insert_many(col_name, docs));

        Ok(result)
    }

    fn insert_many<T: Serialize>(&mut self, col_name: &str, docs: impl IntoIterator<Item = impl Borrow<T>>) -> DbResult<InsertManyResult> {
        let col_meta = self.get_collection_meta_by_name_advanced_auto(col_name, true)?
            .expect("internal: meta must exist");
        let mut inserted_ids: HashMap<usize, Bson> = HashMap::new();
        let mut counter: usize = 0;

        for item in docs {
            let mut doc = bson::to_document(item.borrow())?;
            let insert_one_result = self.insert_one_with_meta(&col_meta, &mut doc)?;
            inserted_ids.insert(counter, insert_one_result.inserted_id);

            counter += 1;
        }

        Ok(InsertManyResult {
            inserted_ids,
        })
    }

    /// query: None for findAll
    pub fn find(&mut self, col_id: u32, meta_version: u32, query: Option<Document>) -> DbResult<DbHandle> {
        self.check_meta_version(meta_version)?;

        let meta_source = self.get_meta_source()?;
        let collection_meta = self.find_collection_root_pid_by_id(
            0, meta_source.meta_pid, col_id)?;

        let subprogram = match query {
            Some(query) => SubProgram::compile_query(
                &collection_meta,
                collection_meta.doc_ref(),
                &query,
                true
            ),
            None => SubProgram::compile_query_all(&collection_meta, true),
        }?;

        let handle = self.make_handle(subprogram);

        Ok(handle)
    }

    pub fn update_many(&mut self, col_id: u32, meta_version: u32, query: Option<&Document>, update: &Document) -> DbResult<usize> {
        self.check_meta_version(meta_version)?;

        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(self, self.internal_update(col_id, query, update, true));

        Ok(result)
    }

    pub fn update_one(&mut self, col_id: u32, meta_version: u32, query: Option<&Document>, update: &Document) -> DbResult<usize> {
        self.check_meta_version(meta_version)?;

        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(self, self.internal_update(col_id, query, update, false));

        Ok(result)
    }

    fn internal_update(&mut self, col_id: u32, query: Option<&Document>, update: &Document, is_many: bool) -> DbResult<usize> {
        let meta_source = self.get_meta_source()?;
        let collection_meta = self.find_collection_root_pid_by_id(
            0, meta_source.meta_pid, col_id)?;

        let subprogram = SubProgram::compile_update(&collection_meta, query, update,
                                                    true, is_many)?;

        let mut vm = VM::new(self.page_handler.as_mut(), Box::new(subprogram));
        vm.execute()?;

        Ok(vm.r2 as usize)
    }

    pub fn drop_collection(&mut self, col_id: u32, meta_version: u32) -> DbResult<()> {
        self.check_meta_version(meta_version)?;

        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        try_db_op!(self, self.internal_drop(col_id));

        Ok(())
    }

    fn internal_drop(&mut self, col_id: u32) -> DbResult<()> {
        let mut meta_source = self.get_meta_source()?;
        let collection_meta = self.find_collection_root_pid_by_id(
            0, meta_source.meta_pid, col_id)?;
        delete_all_helper::delete_all(&mut self.page_handler, collection_meta)?;

        let mut btree_wrapper = BTreePageDeleteWrapper::new(
            self.page_handler.as_mut(), meta_source.meta_pid);

        let pkey = Bson::from(col_id);
        btree_wrapper.delete_item(&pkey)?;

        meta_source.meta_version += 1;
        self.update_meta_source(&meta_source)
    }

    pub fn delete(&mut self, col_id: u32, meta_version: u32, query: Document, is_many: bool) -> DbResult<usize> {
        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        let primary_keys = self.get_primary_keys_by_query(col_id, meta_version,
                                                          Some(query), is_many)?;


        let result = try_db_op!(self, self.internal_delete(col_id, &primary_keys));

        Ok(result)
    }

    fn internal_delete(&mut self, col_id: u32, primary_keys: &[Bson]) -> DbResult<usize> {
        for pkey in primary_keys {
            let _ = self.internal_delete_by_pkey(col_id, pkey)?;
        }

        Ok(primary_keys.len())
    }

    pub fn delete_all(&mut self, col_id: u32, meta_version: u32) -> DbResult<usize> {
        let primary_keys = self.get_primary_keys_by_query(col_id, meta_version,
                                                          None, true)?;

        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(self, self.internal_delete(col_id, &primary_keys));

        Ok(result)
    }

    fn get_primary_keys_by_query(&mut self, col_id: u32, meta_version: u32,
                                 query: Option<Document>, is_many: bool) -> DbResult<Vec<Bson>> {
        let mut handle = self.find(col_id, meta_version, query)?;
        let mut buffer: Vec<Bson> = vec![];

        handle.step()?;

        while handle.has_row() {
            let doc = handle.get().as_document().unwrap();
            let pkey = doc.get("_id").unwrap();
            buffer.push(pkey.clone());

            if !is_many {
                return Ok(buffer);
            }

            handle.step()?;
        }

        Ok(buffer)
    }

    fn update_by_root_pid(&mut self, parent_pid: u32, root_pid: u32, key: &Bson, doc: &Document) -> DbResult<bool> {
        let page = self.page_handler.pipeline_read_page(root_pid)?;
        let btree_node = BTreeNode::from_raw(&page, parent_pid, self.item_size(), self.page_handler.as_mut())?;

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
                                   collection_meta: &mut MetaDocEntry,
                                   left_pid: u32,
                                   backward_item: &InsertBackwardItem) -> DbResult<()> {

        let new_root_id = self.page_handler.alloc_page_id()?;

        crate::polo_log!("handle backward item, left_pid: {}, new_root_id: {}, right_pid: {}", left_pid, new_root_id, backward_item.right_pid);

        let new_root_page = backward_item.write_to_page(self.page_handler.as_mut(), new_root_id, left_pid)?;
        self.page_handler.pipeline_write_page(&new_root_page)?;

        collection_meta.set_root_pid(new_root_id);

        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn delete_by_pkey(&mut self, col_id: u32, key: &Bson) -> DbResult<Option<Rc<Document>>> {
        self.page_handler.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(self, self.internal_delete_by_pkey(col_id, key));

        Ok(result)
    }

    fn internal_delete_by_pkey(&mut self, col_id: u32, key: &Bson) -> DbResult<Option<Rc<Document>>> {
        let meta_source = self.get_meta_source()?;
        let collection_meta = self.find_collection_root_pid_by_id(
            0, meta_source.meta_pid, col_id)?;

        let mut delete_wrapper = BTreePageDeleteWrapper::new(
            self.page_handler.as_mut(),
            collection_meta.root_pid() as u32,
        );
        let result = delete_wrapper.delete_item(key)?;
        delete_wrapper.flush_pages()?;

        if let Some(deleted_item) = &result {
            let index_ctx_opt = IndexCtx::from_meta_doc(collection_meta.doc_ref());
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
        let collection_meta = self.find_collection_root_pid_by_id(
            0, meta_source.meta_pid, col_id)?;
        counter_helper::count(self.page_handler.as_mut(), collection_meta)
    }

    pub(crate) fn query_all_meta(&mut self) -> DbResult<Vec<Document>> {
        let meta_src = self.get_meta_source()?;

        let collection_meta = MetaDocEntry::new(0, "<meta>".into(), meta_src.meta_pid);

        let subprogram = SubProgram::compile_query_all(
            &collection_meta,
            true)?;

        let mut handle = self.make_handle(subprogram);
        handle.step()?;

        let mut result: Vec<Document> = vec![];

        while handle.state() == (VmState::HasRow as i8) {
            let doc = handle.get().as_document().unwrap();

            result.push(doc.clone());

            handle.step()?;
        }

        Ok(result)
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

    pub fn dump(&mut self) -> DbResult<FullDump> {
        let first_page = self.page_handler.pipeline_read_page(0)?;
        let first_page_wrapper = HeaderPageWrapper::from_raw_page(first_page);
        let version = first_page_wrapper.get_version();
        let meta_pid = first_page_wrapper.get_meta_page_id();
        let free_list_pid = first_page_wrapper.get_free_list_page_id();
        let free_list_size = first_page_wrapper.get_free_list_size();
        let page_size = self.page_handler.page_size;

        let journal_dump = self.page_handler.dump_journal()?;
        let full_dump = FullDump {
            identifier: first_page_wrapper.get_title(),
            version: dump_version(&version),
            journal_dump,
            meta_pid,
            free_list_pid,
            free_list_size,
            page_size,
            pages: vec![],
        };
        Ok(full_dump)
    }

    #[allow(dead_code)]
    fn dump_all_pages(&mut self, file_len: u64) -> DbResult<Vec<PageDump>> {
        let page_count = file_len / (self.page_handler.page_size.get() as u64);
        let mut result = Vec::with_capacity(page_count as usize);

        for index in 0..page_count {
            let raw_page = self.page_handler.pipeline_read_page(index as u32)?;
            result.push(dump_page(raw_page)?);
        }

        Ok(result)
    }

}

#[allow(dead_code)]
fn dump_page(raw_page: RawPage) -> DbResult<PageDump> {
    let first = raw_page.data[0];
    let second = raw_page.data[1];
    if first != 0xFF {
        return Ok(PageDump::Undefined(raw_page.page_id));
    }

    let result = match second {
        1 => PageDump::BTreePage(Box::new(BTreePageDump::from_page(&raw_page)?)),
        2 => PageDump::OverflowDataPage(Box::new(OverflowDataPageDump)),
        3 => PageDump::DataPage(Box::new(DataPageDump::from_page(&raw_page)?)),
        4 => PageDump::FreeListPage(Box::new(FreeListPageDump::from_page(raw_page)?)),
        _ => PageDump::Undefined(raw_page.page_id),
    };

    Ok(result)
}

fn dump_version(version: &[u8]) -> String {
    let mut result = String::new();

    let mut i: usize = 0;
    while i < version.len() {
        let digit = version[i] as u32;
        let ch: char = std::char::from_digit(digit, 10).unwrap();
        result.push(ch);
        if i != version.len() - 1 {
            result.push('.');
        }
        i += 1;
    }

    result
}

impl Drop for DbContext {

    fn drop(&mut self) {
        if !self.page_handler.transaction_state().is_no_trans() {
            let _ = self.page_handler.only_rollback_journal();
        }
    }

}
