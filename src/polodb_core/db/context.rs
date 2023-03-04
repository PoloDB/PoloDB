use std::borrow::Borrow;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use bson::{Binary, Bson, DateTime, Document};
use serde::Serialize;
use super::db::DbResult;
use crate::error::DbErr;
use crate::TransactionType;
use crate::Config;
use crate::vm::{SubProgram, VM, VmState};
use crate::meta_doc_helper::meta_doc_key;
// use crate::index_ctx::{IndexCtx, merge_options_into_default};
use crate::btree::*;
use crate::transaction::TransactionState;
use crate::backend::memory::MemoryBackend;
use crate::page::RawPage;
use crate::db::db_handle::DbHandle;
use crate::dump::{BTreePageDump, DataPageDump, FreeListPageDump, FullDump, OverflowDataPageDump, PageDump};
use crate::page::header_page_wrapper::HeaderPageWrapper;
use crate::backend::Backend;
use crate::results::{InsertManyResult, InsertOneResult};
use crate::session::{BaseSession, DynamicSession, Session};
#[cfg(not(target_arch = "wasm32"))]
use crate::backend::file::FileBackend;
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;
#[cfg(target_arch = "wasm32")]
use crate::backend::indexeddb::IndexedDbBackend;
use bson::oid::ObjectId;
use bson::spec::BinarySubtype;
use crate::collection_info::{CollectionSpecification, CollectionSpecificationInfo, CollectionType};
use crate::cursor::Cursor;
use crate::metrics::Metrics;

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
    ($session: tt, $action: expr) => {
        match $action {
            Ok(ret) => {
                $session.auto_commit()?;
                ret
            }

            Err(err) => {
                try_multiple!(err, $session.auto_rollback());
                return Err(err);
            }
        }
    }
}

/**
 * API for all platforms
 */
pub(crate) struct DbContext {
    base_session: BaseSession,
    session_map:  hashbrown::HashMap<ObjectId, Box<dyn Session + Send>>,
    node_id:      [u8; 6],
    metrics:      Metrics,
    #[allow(dead_code)]
    config:       Arc<Config>,
}

#[derive(Debug, Clone, Copy)]
pub struct MetaSource {
    pub meta_pid: u32,
}

impl DbContext {

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_file(path: &Path, config: Config) -> DbResult<DbContext> {
        let metrics = Metrics::new();
        let page_size = NonZeroU32::new(4096).unwrap();

        let config = Arc::new(config);
        let backend = Box::new(FileBackend::open(
            path, page_size, config.clone(), metrics.clone(),
        )?);
        DbContext::open_with_backend(backend, page_size, config, metrics)
    }

    #[cfg(target_arch = "wasm32")]
    pub fn open_indexeddb(ctx: crate::IndexedDbContext, config: Config) -> DbResult<DbContext> {
        let metrics = Metrics::new();
        let page_size = NonZeroU32::new(4096).unwrap();
        let config = Arc::new(config);
        let backend = Box::new(IndexedDbBackend::open(
            ctx, page_size, config.init_block_count
        ));
        DbContext::open_with_backend(backend, page_size, config, metrics)
    }

    pub fn open_memory(config: Config) -> DbResult<DbContext> {
        let metrics = Metrics::new();
        let page_size = NonZeroU32::new(4096).unwrap();
        let config = Arc::new(config);
        let backend = Box::new(MemoryBackend::new(page_size, config.init_block_count));
        DbContext::open_with_backend(backend, page_size, config, metrics)
    }

    fn open_with_backend(
        backend: Box<dyn Backend + Send>,
        page_size: NonZeroU32,
        config: Arc<Config>,
        metrics: Metrics,
    ) -> DbResult<DbContext> {
        let base_session = BaseSession::new(
            backend,
            page_size,
            config.clone(),
            metrics.clone(),
        )?;
        let session_map = hashbrown::HashMap::new();

        let mut node_id: [u8; 6] = [0; 6];
        getrandom::getrandom(&mut node_id).unwrap();

        let ctx = DbContext {
            base_session,
            // first_page,
            node_id,
            session_map,
            metrics,
            config,
        };

        Ok(ctx)
    }

    pub fn metrics(&self) -> Metrics {
        self.metrics.clone()
    }

    pub fn start_session(&mut self) -> DbResult<ObjectId> {
        let id = ObjectId::new();

        let base_session = self.base_session.clone();
        let session = Box::new(DynamicSession::new(
            id.clone(),
            base_session,
            self.metrics.clone_with_sid(id.clone()),
        ));
        let insert_result = self.session_map.insert(id, session);
        if insert_result.is_none() {
            self.base_session.new_session(&id)?;
        }

        Ok(id)
    }

    fn internal_get_collection_id_by_name(session: &dyn Session, name: &str) -> DbResult<CollectionSpecification> {
        let meta_source = DbContext::get_meta_source(session)?;
        DbContext::internal_get_collection_id_by_name_with_pid(session, meta_source.meta_pid, name)
    }

    fn internal_get_collection_id_by_name_with_pid(session: &dyn Session, root_pid: u32, name: &str) -> DbResult<CollectionSpecification> {
        let mut cursor = Cursor::new(root_pid);
        let key = Bson::from(name);

        let reset_result = cursor.reset_by_pkey(session, &key)?;
        if !reset_result {
            return Err(DbErr::CollectionNotFound(name.to_string()));
        }

        let data_ticket_opt = cursor.peek_data();
        if data_ticket_opt.is_none() {
            return Err(DbErr::CollectionNotFound(name.to_string()));
        }

        let data_ticket = data_ticket_opt.unwrap();
        let doc = session.get_doc_from_ticket(&data_ticket)?;
        let entry = bson::from_document::<CollectionSpecification>(doc)?;
        Ok(entry)
    }

    pub fn get_collection_meta_by_name_advanced_auto(
        &mut self,
        name: &str,
        create_if_not_exist: bool,
        session_id: Option<&ObjectId>
    ) -> DbResult<Option<CollectionSpecification>> {
        let session = self.get_session_by_id(session_id)?;
        session.auto_start_transaction(if create_if_not_exist {
            TransactionType::Write
        } else {
            TransactionType::Read
        })?;

        let result = try_db_op!(
            session,
            DbContext::get_collection_meta_by_name_advanced(session, name, create_if_not_exist, &self.node_id)
        );

        Ok(result)
    }

    pub fn get_collection_meta_by_name_advanced(session: &dyn Session, name: &str, create_if_not_exist: bool, node_id: &[u8; 6]) -> DbResult<Option<CollectionSpecification>> {
        match DbContext::internal_get_collection_id_by_name(session, name) {
            Ok(meta) => Ok(Some(meta)),
            Err(DbErr::CollectionNotFound(_)) => {
                if create_if_not_exist {
                    let meta = DbContext::internal_create_collection(session, name, node_id)?;
                    Ok(Some(meta))
                } else {
                    Ok(None)
                }
            },
            Err(err) => return Err(err),
        }
    }

    fn get_meta_source(session: &dyn Session) -> DbResult<MetaSource> {
        let head_page = session.read_page(0)?;
        DbContext::check_first_page_valid(&head_page)?;
        let head_page_wrapper = HeaderPageWrapper::from_raw_page(head_page.as_ref().clone());
        let meta_pid = head_page_wrapper.get_meta_page_id();
        Ok(MetaSource {
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

    fn get_session_by_id(&self, session_id: Option<&ObjectId>) -> DbResult<&dyn Session> {
        match session_id {
            Some(session_id) => {
                let session = match self.session_map.get(session_id) {
                    Some(session) => session.as_ref(),
                    None => {
                        let err = DbErr::InvalidSession(Box::new(session_id.clone()));
                        return Err(err);
                    }
                };
                Ok(session)
            }

            None => Ok(&self.base_session)
        }
    }

    pub fn create_collection(&mut self, name: &str, session_id: Option<&ObjectId>) -> DbResult<CollectionSpecification> {
        let session = self.get_session_by_id(session_id)?;
        session.auto_start_transaction(TransactionType::Write)?;

        let meta = try_db_op!(session, DbContext::internal_create_collection(session, name, &self.node_id));

        Ok(meta)
    }

    fn check_collection_exist(session: &dyn Session, name: &str) -> DbResult<bool> {
        let test_collection = DbContext::internal_get_collection_id_by_name(session, name);
        match test_collection {
            Ok(_) => Ok(true),
            Err(DbErr::CollectionNotFound(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn internal_create_collection(session: &dyn Session, name: &str, node_id: &[u8; 6]) -> DbResult<CollectionSpecification> {
        if name.is_empty() {
            return Err(DbErr::IllegalCollectionName(name.into()));
        }
        let exist = DbContext::check_collection_exist(session, name)?;
        if exist {
            return Err(DbErr::CollectionAlreadyExits(name.into()));
        }

        let mut meta_source = DbContext::get_meta_source(session)?;
        let root_pid = session.alloc_page_id()?;

        let uuid = uuid::Uuid::now_v1(node_id);

        let spec = CollectionSpecification {
            _id: name.to_string(),
            collection_type: CollectionType::Collection,
            info: CollectionSpecificationInfo {
                uuid: Some(Binary {
                    subtype: BinarySubtype::Uuid,
                    bytes: uuid.as_bytes().to_vec(),
                }),

                create_at: DateTime::now(),

                root_pid,
            },
            indexes: HashMap::new(),
        };

        let mut btree_wrapper = BTreePageInsertWrapper::new(
            session,
            meta_source.meta_pid,
        );

        let spec_doc = bson::to_document(&spec)?;
        let insert_result = btree_wrapper.insert_item(&spec_doc, false)?;

        // if a backward item returns, it's saying that the btree has been "rotated".
        // the center node of the btree has been changed.
        // So you have to distribute a new page to store the "central node",
        // and the newer page is the center of the btree.
        if let Some(backward_item) = insert_result.backward_item {
            let new_root_id = session.alloc_page_id()?;

            let raw_page = backward_item.write_to_page(
                session,
                new_root_id,
                meta_source.meta_pid
            )?;
            session.write_page(&raw_page)?;

            meta_source.meta_pid = new_root_id;
        }

        Ok(spec)
    }

    fn update_meta_source(session: &dyn Session, meta_source: &MetaSource) -> DbResult<()> {
        let head_page = session.read_page(0)?;
        let mut head_page_wrapper = HeaderPageWrapper::from_raw_page(head_page.as_ref().clone());
        head_page_wrapper.set_meta_page_id(meta_source.meta_pid);
        session.write_page(&head_page_wrapper.0)
    }

    pub(crate) fn make_handle(session: &dyn Session, program: SubProgram) -> DbHandle {
        let vm = VM::new(session, program);
        DbHandle::new(vm)
    }

    pub fn create_index(&mut self, col_id: u32, keys: &Document, options: Option<&Document>, session_id: Option<&ObjectId>) -> DbResult<()> {
        let session = self.get_session_by_id(session_id)?;
        session .auto_start_transaction(TransactionType::Write)?;

        try_db_op!(session, DbContext::internal_create_index(session, col_id, keys, options));

        Ok(())
    }

    fn internal_create_index(_session: &dyn Session, _col_id: u32, _keys: &Document, _options: Option<&Document>) -> DbResult<()> {
        unimplemented!()
        // let meta_source = DbContext::get_meta_source(session)?;
        // let mut meta_doc = DbContext::find_collection_root_pid_by_id(
        //     session, 0, meta_source.meta_pid, col_id
        // )?;
        //
        // for (key_name, value_of_key) in keys.iter() {
        //     if let Bson::Int32(1) = value_of_key {
        //         // nothing
        //     } else if let Bson::Int64(1) = value_of_key {
        //         // nothing
        //     } else {
        //         return Err(DbErr::InvalidOrderOfIndex(key_name.clone()));
        //     }
        //
        //     match meta_doc.doc_ref().get(meta_doc_key::INDEXES) {
        //         Some(indexes_obj) => match indexes_obj {
        //             Bson::Document(index_doc) => {
        //                 if index_already_exists(index_doc.borrow(), key_name) {
        //                     return Err(DbErr::IndexAlreadyExists(key_name.clone()));
        //                 }
        //
        //                 unimplemented!()
        //             }
        //
        //             _ => {
        //                 panic!("unexpected: indexes object is not a Document");
        //             }
        //
        //         },
        //
        //         None => {
        //             // create indexes
        //             let mut doc = doc!();
        //
        //             let root_pid = session.alloc_page_id()?;
        //             let options_doc = merge_options_into_default(root_pid, options)?;
        //             doc.insert(key_name.clone(), Bson::Document(options_doc));
        //
        //             meta_doc.set_indexes(doc);
        //         }
        //
        //     }
        // }
        //
        // let key_col = Bson::from(col_id);
        //
        // let meta_source = DbContext::get_meta_source(session)?;
        // let inserted = DbContext::update_by_root_pid(
        //     session, 0, meta_source.meta_pid,
        //     &key_col, meta_doc.doc_ref()
        // )?;
        // if !inserted {
        //     panic!("update failed");
        // }
        //
        // Ok(())
    }

    #[inline]
    fn fix_doc(mut doc: Document) -> Document {
        if doc.get(meta_doc_key::ID).is_some() {
            return doc;
        }

        let new_oid = ObjectId::new();
        doc.insert::<String, Bson>(meta_doc_key::ID.into(), new_oid.into());
        doc
    }

    pub fn insert_one_auto(&mut self, col_name: &str, doc: Document, session_id: Option<&ObjectId>) -> DbResult<InsertOneResult> {
        let session = self.get_session_by_id(session_id)?;
        session.auto_start_transaction(TransactionType::Write)?;

        let changed = try_db_op!(session, DbContext::insert_one(session, col_name, doc, &self.node_id));

        Ok(changed)
    }

    fn insert_one(session: &dyn Session, col_name: &str, doc: Document, node_id: &[u8; 6]) -> DbResult<InsertOneResult> {
        let col_meta = DbContext::get_collection_meta_by_name_advanced(session, col_name, true, node_id)?
            .expect("internal: meta must exist");
        let (result, _) = DbContext::insert_one_with_meta(session, col_meta, doc)?;
        Ok(result)
    }

    /// Insert one item with the collection spec
    /// return the new spec for the outside to do the following operation
    fn insert_one_with_meta(session: &dyn Session, mut col_spec: CollectionSpecification, doc: Document) -> DbResult<(InsertOneResult, CollectionSpecification)> {
        let col_id = col_spec.name().to_string();

        let meta_source = DbContext::get_meta_source(session)?;
        let doc  = DbContext::fix_doc(doc);

        let pkey = doc.get("_id").unwrap();

        // let mut is_pkey_check_skipped = false;
        // collection_meta.check_pkey_ty(&pkey, &mut is_pkey_check_skipped)?;

        let mut is_meta_changed = false;

        // // insert index begin
        // let mut index_ctx_opt = IndexCtx::from_meta_doc(col_spec);
        // if let Some(index_ctx) = &mut index_ctx_opt {
        //     let mut is_ctx_changed = false;
        //
        //     index_ctx.insert_index_by_content(
        //         &doc,
        //         &pkey,
        //         &mut is_ctx_changed,
        //         session,
        //     )?;
        //
        //     if is_ctx_changed {
        //         index_ctx.merge_to_meta_doc(&mut collection_meta);
        //         is_meta_changed = true;
        //     }
        // }
        // // insert index end

        let mut insert_wrapper = BTreePageInsertWrapper::new(
            session,
            col_spec.info.root_pid,
        );
        let insert_result: InsertResult = insert_wrapper.insert_item(&doc, false)?;

        if let Some(backward_item) = &insert_result.backward_item {
            let root_pid = col_spec.info.root_pid;
            DbContext::handle_insert_backward_item(
                session, &mut col_spec,
                root_pid, backward_item
            )?;
            is_meta_changed = true;
        }

        // // insert successfully
        // if is_pkey_check_skipped {
        //     collection_meta.merge_pkey_ty_to_meta(&doc);
        //     is_meta_changed = true;
        // }

        // update meta begin
        if is_meta_changed {
            let key = Bson::from(col_id);
            let doc = bson::to_document(&col_spec)?;
            let updated= DbContext::update_by_root_pid(
                session,
                meta_source.meta_pid,
                &key,
                &doc,
            )?;
            if !updated {
                panic!("unexpected: update meta page failed")
            }
        }
        // update meta end

        Ok((
            InsertOneResult { inserted_id: pkey.clone() },
            col_spec
        ))
    }

    pub fn insert_many_auto<T: Serialize>(
        &mut self,
        col_name: &str,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        session_id: Option<&ObjectId>
    ) -> DbResult<InsertManyResult> {
        let session = self.get_session_by_id(session_id)?;
        session.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(session, DbContext::insert_many(session, col_name, docs, &self.node_id));

        Ok(result)
    }

    fn insert_many<T: Serialize>(
        session: &dyn Session,
        col_name: &str,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        node_id: &[u8; 6],
    ) -> DbResult<InsertManyResult> {
        let mut col_spec = DbContext::get_collection_meta_by_name_advanced(session, col_name, true, node_id)?
            .expect("internal: meta must exist");
        let mut inserted_ids: HashMap<usize, Bson> = HashMap::new();
        let mut counter: usize = 0;

        for item in docs {
            let doc = bson::to_document(item.borrow())?;
            let (insert_one_result, new_col_spec) = DbContext::insert_one_with_meta(session, col_spec, doc)?;
            inserted_ids.insert(counter, insert_one_result.inserted_id);

            counter += 1;
            col_spec = new_col_spec;
        }

        Ok(InsertManyResult {
            inserted_ids,
        })
    }

    /// query: None for findAll
    pub fn find(&mut self, col_spec: &CollectionSpecification, query: Option<Document>, session_id: Option<&ObjectId>) -> DbResult<DbHandle> {
        let session = self.get_session_by_id(session_id)?;
        DbContext::find_internal(session, col_spec, query)
    }

    fn find_internal<'a, 'b>(session: &'a dyn Session, col_spec: &'b CollectionSpecification, query: Option<Document>) -> DbResult<DbHandle<'a>> {
        // let meta_source = DbContext::get_meta_source(session)?;
        // let collection_meta = DbContext::find_collection_root_pid_by_id(
        //     session, 0,
        //     meta_source.meta_pid, col_id
        // )?;

        let subprogram = match query {
            Some(query) => SubProgram::compile_query(
                col_spec,
                &query,
                true
            ),
            None => SubProgram::compile_query_all(col_spec, true),
        }?;

        let handle = DbContext::make_handle(session, subprogram);
        Ok(handle)
    }

    pub fn update_many(&mut self, col_spec: &CollectionSpecification, query: Option<&Document>, update: &Document, session_id: Option<&ObjectId>) -> DbResult<usize> {
        let session = self.get_session_by_id(session_id)?;
        session.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(session, DbContext::internal_update(session, col_spec, query, update, true));

        Ok(result)
    }

    pub fn update_one(&mut self, col_spec: &CollectionSpecification, query: Option<&Document>, update: &Document, session_id: Option<&ObjectId>) -> DbResult<usize> {
        let session = self.get_session_by_id(session_id)?;
        session.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(session, DbContext::internal_update(session, col_spec, query, update, false));

        Ok(result)
    }

    fn internal_update(session: &dyn Session, col_spec: &CollectionSpecification, query: Option<&Document>, update: &Document, is_many: bool) -> DbResult<usize> {
        let subprogram = SubProgram::compile_update(
            col_spec,
            query,
            update,
            true,
            is_many,
        )?;

        let mut vm = VM::new(session, subprogram);
        vm.execute()?;

        Ok(vm.r2 as usize)
    }

    pub fn drop_collection(&mut self, name: &str, session_id: Option<&ObjectId>) -> DbResult<()> {
        let session = self.get_session_by_id(session_id)?;
        session.auto_start_transaction(TransactionType::Write)?;

        try_db_op!(session, DbContext::internal_drop(session, name));

        Ok(())
    }

    fn internal_drop(session: &dyn Session, name: &str) -> DbResult<()> {
        let meta_source = DbContext::get_meta_source(session)?;
        let collection_meta = DbContext::internal_get_collection_id_by_name(session, name)?;
        delete_all_helper::delete_all(session, &collection_meta)?;

        let mut btree_wrapper = BTreePageDeleteWrapper::new(
            session, meta_source.meta_pid);

        let pkey = Bson::from(name);
        btree_wrapper.delete_item(&pkey)?;

        DbContext::update_meta_source(session, &meta_source)
    }

    pub fn delete(&mut self, col_name: &str, query: Document, is_many: bool, session_id: Option<&ObjectId>) -> DbResult<usize> {
        let session = self.get_session_by_id(session_id)?;
        session.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(session, DbContext::internal_delete_by_query(session, col_name, query, is_many));

        Ok(result)
    }

    fn internal_delete(session: &dyn Session, col_name: &str, primary_keys: &[Bson]) -> DbResult<usize> {
        let mut count: usize = 0;
        for pkey in primary_keys {
            let delete_result = DbContext::internal_delete_by_pkey(session, col_name, pkey)?;
            if delete_result.is_some() {
                count += 1;
            }
        }

        Ok(count)
    }

    fn internal_delete_by_query(session: &dyn Session, col_name: &str, query: Document, is_many: bool) -> DbResult<usize> {
        let primary_keys = DbContext::get_primary_keys_by_query(
            session,
            col_name,
            Some(query),
            is_many,
        )?;
        DbContext::internal_delete(session, col_name, &primary_keys)
    }

    fn internal_delete_all(session: &dyn Session, col_name: &str) -> DbResult<usize> {
        let primary_keys = DbContext::get_primary_keys_by_query(
            session,
            col_name,
            None,
            true,
        )?;
        DbContext::internal_delete(session, col_name, &primary_keys)
    }

    pub fn delete_all(&mut self, col_name: &str, session_id: Option<&ObjectId>) -> DbResult<usize> {
        let session = self.get_session_by_id(session_id)?;
        session.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(session, DbContext::internal_delete_all(session, col_name));

        Ok(result)
    }

    fn get_primary_keys_by_query(session: &dyn Session, col_name: &str, query: Option<Document>, is_many: bool) -> DbResult<Vec<Bson>> {
        let col_spec = DbContext::internal_get_collection_id_by_name(session, col_name)?;
        let mut handle = DbContext::find_internal(session, &col_spec, query)?;
        let mut buffer: Vec<Bson> = vec![];

        handle.step()?;

        while handle.has_row() {
            let doc = handle.get().as_document().unwrap();
            let pkey = doc.get("_id").unwrap();
            buffer.push(pkey.clone());

            if !is_many {
                handle.commit_and_close_vm()?;
                return Ok(buffer);
            }

            handle.step()?;
        }

        handle.commit_and_close_vm()?;
        Ok(buffer)
    }

    fn update_by_root_pid(session: &dyn Session, root_pid: u32, key: &Bson, doc: &Document) -> DbResult<bool> {
        let mut cursor = Cursor::new(root_pid);

        let reset_result = cursor.reset_by_pkey(session, key)?;

        if !reset_result {
            return Ok(false);
        }

        cursor.update_current(session, doc)?;

        Ok(true)
    }

    fn handle_insert_backward_item(session: &dyn Session,
                                   col_spec: &mut CollectionSpecification,
                                   left_pid: u32,
                                   backward_item: &InsertBackwardItem
    ) -> DbResult<()> {
        let new_root_id = session.alloc_page_id()?;

        crate::polo_log!("handle backward item, left_pid: {}, new_root_id: {}, right_pid: {}", left_pid, new_root_id, backward_item.right_pid);

        let new_root_page = backward_item.write_to_page(session, new_root_id, left_pid)?;
        session.write_page(&new_root_page)?;

        col_spec.info.root_pid = new_root_id;

        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn delete_by_pkey(&mut self, col_name: &str, key: &Bson, session_id: Option<&ObjectId>) -> DbResult<Option<Document>> {
        let session = self.get_session_by_id(session_id)?;
        session.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(session, DbContext::internal_delete_by_pkey(session, col_name, key));

        Ok(result)
    }

    fn internal_delete_by_pkey(session: &dyn Session, col_name: &str, key: &Bson) -> DbResult<Option<Document>> {
        let collection_meta = DbContext::internal_get_collection_id_by_name(
            session, col_name,
        )?;

        let mut delete_wrapper = BTreePageDeleteWrapper::new(
            session,
            collection_meta.info.root_pid as u32,
        );
        let result = delete_wrapper.delete_item(key)?;
        delete_wrapper.flush_pages()?;

        if let Some(_deleted_item) = &result {
            // let index_ctx_opt = IndexCtx::from_meta_doc(collection_meta.doc_ref());
            // if let Some(index_ctx) = &index_ctx_opt {
            //     index_ctx.delete_index_by_content(deleted_item.borrow(), session)?;
            // }

            return Ok(result)
        }

        Ok(None)
    }

    pub fn count(&mut self, name: &str, session_id: Option<&ObjectId>) -> DbResult<u64> {
        let session = self.get_session_by_id(session_id)?;
        DbContext::count_internal(session, name)
    }

    fn count_internal(session: &dyn Session, name: &str) -> DbResult<u64> {
        let col_spec = DbContext::internal_get_collection_id_by_name(session, name)?;
        counter_helper::count(session, &col_spec)
    }

    pub(crate) fn query_all_meta(&mut self, session_id: Option<&ObjectId>) -> DbResult<Vec<Document>> {
        let session = self.get_session_by_id(session_id)?;
        DbContext::query_all_meta_internal(session)
    }

    fn query_all_meta_internal(session: &dyn Session) -> DbResult<Vec<Document>> {
        let meta_src = DbContext::get_meta_source(session)?;

        let col_spec = CollectionSpecification {
            _id: "<meta>".into(),
            collection_type: CollectionType::Collection,
            info: CollectionSpecificationInfo {
                uuid: None,
                create_at: DateTime::now(),
                root_pid: meta_src.meta_pid,
            },
            indexes: HashMap::new(),
        };

        let subprogram = SubProgram::compile_query_all(
            &col_spec, true,
        )?;

        let mut handle = DbContext::make_handle(session, subprogram);
        handle.step()?;

        let mut result: Vec<Document> = vec![];

        while handle.state() == (VmState::HasRow as i8) {
            let doc = handle.get().as_document().unwrap();

            result.push(doc.clone());

            handle.step()?;
        }

        Ok(result)
    }

    pub fn start_transaction(&mut self, ty: Option<TransactionType>, session_id: Option<&ObjectId>) -> DbResult<()> {
        if session_id.is_none() {
            match ty {
                Some(ty) => {
                    self.base_session.start_transaction(ty)?;
                    self.base_session.set_transaction_state(TransactionState::User);
                }

                None => {
                    self.base_session.start_transaction(TransactionType::Read)?;
                    self.base_session.set_transaction_state(TransactionState::UserAuto);
                }

            }
        } else {
            let session = self.get_session_by_id(session_id)?;
            session.start_transaction(ty.unwrap_or(TransactionType::Read))?;
        }
        Ok(())
    }

    pub fn commit(&mut self, session_id: Option<&ObjectId>) -> DbResult<()> {
        if session_id.is_none() {
            self.base_session.commit()?;
            self.base_session.set_transaction_state(TransactionState::NoTrans);
        } else {
            let session = self.get_session_by_id(session_id)?;
            session.commit()?;
        }
        Ok(())
    }

    pub fn rollback(&mut self, session_id: Option<&ObjectId>) -> DbResult<()> {
        if session_id.is_none() {
            self.base_session.rollback()?;
            self.base_session.set_transaction_state(TransactionState::NoTrans);
        } else {
            let session = self.get_session_by_id(session_id)?;
            session.rollback()?;
        }
        Ok(())
    }

    pub fn drop_session(&mut self, session_id: &ObjectId) -> DbResult<()> {
        let remove_result = self.session_map.remove(session_id);
        if remove_result.is_some() {
            self.base_session.remove_session(session_id)?;
        }
        Ok(())
    }

    pub fn dump(&mut self) -> DbResult<FullDump> {
        let first_page = self.base_session.read_page(0)?;
        let first_page_wrapper = HeaderPageWrapper::from_raw_page(first_page.as_ref().clone());
        let version = first_page_wrapper.get_version();
        let meta_pid = first_page_wrapper.get_meta_page_id();
        let free_list_pid = first_page_wrapper.get_free_list_page_id();
        let free_list_size = first_page_wrapper.get_free_list_size();
        let page_size = self.base_session.page_size();

        let journal_dump = self.base_session.dump_journal()?;
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
        let page_count = file_len / (self.base_session.page_size().get() as u64);
        let mut result = Vec::with_capacity(page_count as usize);

        for index in 0..page_count {
            let raw_page = self.base_session.read_page(index as u32)?;
            result.push(dump_page(raw_page.as_ref().clone())?);
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
        if !self.base_session.transaction_state().is_no_trans() {
            let _ = self.base_session.only_rollback_journal();
        }
    }

}
