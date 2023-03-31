/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::borrow::Borrow;
use std::collections::HashMap;
use std::num::{NonZeroU32, NonZeroU64};
use bson::{Binary, Bson, DateTime, Document};
use serde::Serialize;
use super::db::DbResult;
use crate::error::DbErr;
use crate::{LsmKv, TransactionType};
use crate::Config;
use crate::vm::{SubProgram, VM, VmState};
use crate::meta_doc_helper::meta_doc_key;
// use crate::index_ctx::{IndexCtx, merge_options_into_default};
use crate::transaction::TransactionState;
use crate::backend::memory::MemoryBackend;
use crate::page::RawPage;
use crate::db::db_handle::DbHandle;
use crate::page::header_page_wrapper::HeaderPageWrapper;
use crate::backend::Backend;
use crate::results::{InsertManyResult, InsertOneResult};
use crate::session::{BaseSession, DynamicSession, Session};
#[cfg(not(target_arch = "wasm32"))]
use crate::backend::file::FileBackend;
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;
use std::sync::{Arc, Mutex};
#[cfg(target_arch = "wasm32")]
use crate::backend::indexeddb::IndexedDbBackend;
use bson::oid::ObjectId;
use bson::spec::BinarySubtype;
use crate::collection_info::{CollectionSpecification, CollectionSpecificationInfo, CollectionType};
use crate::cursor::Cursor;
use crate::lsm::LsmSession;
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
    ($self: tt, $session: expr, $action: expr) => {
        match $action {
            Ok(ret) => {
                $self.auto_commit($session)?;
                ret
            }

            Err(err) => {
                try_multiple!(err, $self.auto_rollback($session));
                return Err(err);
            }
        }
    }
}

pub(crate) struct SessionInner {
    pub(crate) kv_session: LsmSession,
    auto_count: i32,
}

impl SessionInner {

    pub fn new(kv_session: LsmSession) -> SessionInner {
        SessionInner {
            kv_session,
            auto_count: 0,
        }
    }

    pub fn auto_start_transaction(&mut self, ty: TransactionType) -> DbResult<()> {
        if self.auto_count == 0 && self.kv_session.transaction().is_some() {
            if self.kv_session.transaction().is_some() {  // manually
                return Ok(());
            }

            self.kv_session.start_transaction(ty)?;  // auto
        }

        self.auto_count += 1;

        Ok(())
    }

    pub fn auto_commit(&mut self, kv_engine: &LsmKv) -> DbResult<()> {
        if self.auto_count == 0 {
            return Ok(());
        }

        self.auto_count -= 1;

        if self.auto_count == 0 {
            kv_engine.inner.commit(&mut self.kv_session)?;
        }

        Ok(())
    }

}

const TABLE_META_PREFIX: &'static str = "$TABLE_META";

/**
 * API for all platforms
 */
pub(crate) struct DbContext {
    kv_engine:    LsmKv,
    session_map:  HashMap<ObjectId, Arc<Mutex<SessionInner>>>,
    node_id:      [u8; 6],
    metrics:      Metrics,
    #[allow(dead_code)]
    config:       Config,
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

        let kv_engine = LsmKv::open_file(path)?;

        let backend = Box::new(FileBackend::open(
            path, page_size, config.clone(), metrics.clone(),
        )?);
        DbContext::open_with_backend(
            kv_engine,
            backend,
            page_size,
            config,
            metrics,
        )
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
        let backend = Box::new(MemoryBackend::new(
            page_size,
            NonZeroU64::new(config.get_init_block_count()).unwrap(),
        ));
        let kv_engine = LsmKv::open_memory()?;
        DbContext::open_with_backend(
            kv_engine,
            backend,
            page_size,
            config,
            metrics,
        )
    }

    fn open_with_backend(
        kv_engine: LsmKv,
        backend: Box<dyn Backend + Send>,
        page_size: NonZeroU32,
        config: Config,
        metrics: Metrics,
    ) -> DbResult<DbContext> {
        let mut node_id: [u8; 6] = [0; 6];
        getrandom::getrandom(&mut node_id).unwrap();

        let ctx = DbContext {
            kv_engine,
            session_map: HashMap::new(),
            // first_page,
            node_id,
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

        let kv_session = self.kv_engine.new_session();
        let inner = SessionInner::new(kv_session);
        self.session_map.insert(id, Arc::new(Mutex::new(inner)));

        Ok(id)
    }

    fn internal_get_collection_id_by_name(&self, session: &SessionInner, name: &str) -> DbResult<CollectionSpecification> {
        let mut cursor =  {
            let kv_cursor = self.kv_engine.open_multi_cursor(Some(&session.kv_session));
            Cursor::new(TABLE_META_PREFIX.to_string(), kv_cursor)
        };

        let key = Bson::from(name);

        let reset_result = cursor.reset_by_pkey(&key)?;
        if !reset_result {
            return Err(DbErr::CollectionNotFound(name.to_string()));
        }

        let data = cursor.peek_data(self.kv_engine.inner.as_ref())?;
        if data.is_none() {
            return Err(DbErr::CollectionNotFound(name.to_string()));
        }

        let entry = bson::from_slice::<CollectionSpecification>(data.unwrap().as_ref())?;
        Ok(entry)
    }

    fn get_session_by_id(&mut self, session_id: Option<&ObjectId>) -> DbResult<&Arc<Mutex<SessionInner>>> {
        match session_id {
            None => {
                let sid = self.start_session()?;
                Ok(self.session_map.get(&sid).unwrap())
            }
            Some(sid) => {
                Ok(self.session_map.get(sid).unwrap())
            }
        }
    }

    pub fn get_collection_meta_by_name_advanced_auto_by_id(
        &mut self,
        name: &str,
        create_if_not_exist: bool,
        session_id: Option<&ObjectId>
    ) -> DbResult<Option<CollectionSpecification>> {
        let session_ref = self.get_session_by_id(session_id)?.clone();

        let mut session = session_ref.lock()?;
        self.get_collection_meta_by_name_advanced_auto(name, create_if_not_exist, &mut session)
    }

    pub fn get_collection_meta_by_name_advanced_auto(
        &mut self,
        name: &str,
        create_if_not_exist: bool,
        session: &mut SessionInner,
    ) -> DbResult<Option<CollectionSpecification>> {
        self.auto_start_transaction(session, if create_if_not_exist {
            TransactionType::Write
        } else {
            TransactionType::Read
        })?;

        let result = try_db_op!(
            self,
            session,
            DbContext::get_collection_meta_by_name_advanced(self, session, name, create_if_not_exist, &self.node_id)
        );

        Ok(result)
    }

    pub fn get_collection_meta_by_name_advanced(&self, session: &mut SessionInner, name: &str, create_if_not_exist: bool, node_id: &[u8; 6]) -> DbResult<Option<CollectionSpecification>> {
        match self.internal_get_collection_id_by_name(session, name) {
            Ok(meta) => Ok(Some(meta)),
            Err(DbErr::CollectionNotFound(_)) => {
                if create_if_not_exist {
                    let meta = self.internal_create_collection(session, name, node_id)?;
                    Ok(Some(meta))
                } else {
                    Ok(None)
                }
            },
            Err(err) => return Err(err),
        }
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

    fn auto_start_transaction(&mut self, session: &mut SessionInner, ty: TransactionType) -> DbResult<()> {
        session.kv_session.start_transaction(ty)
    }

    fn auto_commit(&self, session: &mut SessionInner) -> DbResult<()> {
        session.auto_commit(&self.kv_engine)?;
        Ok(())
    }

    fn auto_rollback(&self, session: &mut SessionInner) -> DbResult<()> {
        unimplemented!()
    }

    pub fn create_collection_by_id(&mut self, name: &str, session_id: Option<&ObjectId>) -> DbResult<CollectionSpecification> {
        let session_ref = self.get_session_by_id(session_id)?.clone();
        let mut session = session_ref.lock()?;
        self.create_collection(name, &mut session)
    }

    pub fn create_collection(&mut self, name: &str, session: &mut SessionInner) -> DbResult<CollectionSpecification> {
        self.auto_start_transaction(session, TransactionType::Write)?;

        let meta = try_db_op!(self, session, self.internal_create_collection(session, name, &self.node_id));

        Ok(meta)
    }

    fn check_collection_exist(&self, session: &mut SessionInner, name: &str) -> DbResult<bool> {
        let test_collection = self.internal_get_collection_id_by_name(session, name);
        match test_collection {
            Ok(_) => Ok(true),
            Err(DbErr::CollectionNotFound(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn internal_create_collection(&self, session: &mut SessionInner, name: &str, node_id: &[u8; 6]) -> DbResult<CollectionSpecification> {
        if name.is_empty() {
            return Err(DbErr::IllegalCollectionName(name.into()));
        }
        let exist = self.check_collection_exist(session, name)?;
        if exist {
            return Err(DbErr::CollectionAlreadyExits(name.into()));
        }

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
            },
            indexes: HashMap::new(),
        };

        let stacked_key = crate::utils::bson::stacked_key(&[
            Bson::String(TABLE_META_PREFIX.to_string()),
            Bson::String(name.to_string()),
        ])?;

        let buffer = bson::to_vec(&spec)?;

        session.kv_session.put(stacked_key.as_slice(), buffer.as_ref())?;

        Ok(spec)
    }

    pub(crate) fn make_handle(&mut self, session: Arc<Mutex<SessionInner>>, program: SubProgram) -> DbResult<DbHandle> {
        let vm = VM::new(self.kv_engine.clone(), session, program);
        Ok(DbHandle::new(vm))
    }

    pub fn create_index_id(&mut self, prefix: Bson, keys: &Document, options: Option<&Document>, session_id: Option<&ObjectId>) -> DbResult<()> {
        let session_ref = self.get_session_by_id(session_id)?.clone();
        let mut session = session_ref.lock()?;
        self.create_index(prefix, keys, options, &mut session)
    }

    pub fn create_index(&mut self, prefix: Bson, keys: &Document, options: Option<&Document>, session: &mut SessionInner) -> DbResult<()> {
        self.auto_start_transaction(session, TransactionType::Write)?;

        try_db_op!(self, session, DbContext::internal_create_index(session, prefix, keys, options));

        Ok(())
    }

    fn internal_create_index(_session: &mut SessionInner, _prefix: Bson, _keys: &Document, _options: Option<&Document>) -> DbResult<()> {
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

    pub fn insert_one_auto_by_id(&mut self, col_name: &str, doc: Document, session_id: Option<&ObjectId>) -> DbResult<InsertOneResult> {
        let session_ref = self.get_session_by_id(session_id)?.clone();
        let mut session = session_ref.lock()?;
        self.insert_one_auto(col_name, doc, &mut session)
    }

    pub fn insert_one_auto(&mut self, col_name: &str, doc: Document, session: &mut SessionInner) -> DbResult<InsertOneResult> {
        self.auto_start_transaction(session, TransactionType::Write)?;

        let changed = try_db_op!(self, session, self.insert_one(session, col_name, doc, &self.node_id));

        Ok(changed)
    }

    fn insert_one(&self, session: &mut SessionInner, col_name: &str, doc: Document, node_id: &[u8; 6]) -> DbResult<InsertOneResult> {
        let col_meta = self.get_collection_meta_by_name_advanced(session, col_name, true, node_id)?
            .expect("internal: meta must exist");
        let (result, _) = self.insert_one_with_meta(session, col_meta, doc)?;
        Ok(result)
    }

    /// Insert one item with the collection spec
    /// return the new spec for the outside to do the following operation
    fn insert_one_with_meta(&self, session: &mut SessionInner, mut col_spec: CollectionSpecification, doc: Document) -> DbResult<(InsertOneResult, CollectionSpecification)> {
        let doc  = DbContext::fix_doc(doc);

        let pkey = doc.get("_id").unwrap();

        let stacked_key = crate::utils::bson::stacked_key([
            &Bson::String(col_spec._id.clone()),
            &pkey,
        ])?;

        let doc_buf = bson::to_vec(&doc)?;

        session.kv_session.put(
            stacked_key.as_ref(),
            &doc_buf,
        )?;

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


        Ok((
            InsertOneResult { inserted_id: pkey.clone() },
            col_spec
        ))
    }

    pub fn insert_many_auto_by_id<T: Serialize>(
        &mut self,
        col_name: &str,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        session_id: Option<&ObjectId>
    ) -> DbResult<InsertManyResult> {
        let session_ref = self.get_session_by_id(session_id)?.clone();
        let mut session = session_ref.lock()?;

        self.insert_many_auto(col_name, docs, &mut session)
    }

    pub fn insert_many_auto<T: Serialize>(
        &mut self,
        col_name: &str,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        session: &mut SessionInner
    ) -> DbResult<InsertManyResult> {
        self.auto_start_transaction(session, TransactionType::Write)?;

        let result = try_db_op!(self, session, self.insert_many(session, col_name, docs, &self.node_id));

        Ok(result)
    }

    fn insert_many<T: Serialize>(
        &self,
        session: &mut SessionInner,
        col_name: &str,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        node_id: &[u8; 6],
    ) -> DbResult<InsertManyResult> {
        let mut col_spec = self.get_collection_meta_by_name_advanced(session, col_name, true, node_id)?
            .expect("internal: meta must exist");
        let mut inserted_ids: HashMap<usize, Bson> = HashMap::new();
        let mut counter: usize = 0;

        for item in docs {
            let doc = bson::to_document(item.borrow())?;
            let (insert_one_result, new_col_spec) = self.insert_one_with_meta(session, col_spec, doc)?;
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
        let session = match session_id {
            Some(sid) => {
                self.session_map.get(sid).unwrap().clone()
            }
            None => {
                let oid = self.start_session()?;
                self.session_map.get(&oid).unwrap().clone()
            }
        };
        self.find_internal(session, col_spec, query)
    }

    fn find_internal(&mut self, session: Arc<Mutex<SessionInner>>, col_spec: &CollectionSpecification, query: Option<Document>) -> DbResult<DbHandle> {
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

        let handle = self.make_handle(session, subprogram)?;
        Ok(handle)
    }

    pub fn update_many_id(&mut self, col_spec: &CollectionSpecification, query: Option<&Document>, update: &Document, session_id: Option<&ObjectId>) -> DbResult<usize> {
        let session = self.get_session_by_id(session_id)?.clone();
        self.update_many(col_spec, query, update, session.clone())
    }

    pub fn update_many(&mut self, col_spec: &CollectionSpecification, query: Option<&Document>, update: &Document, session: Arc<Mutex<SessionInner>>) -> DbResult<usize> {
        let result = self.internal_update(session, col_spec, query, update, true)?;
        Ok(result)
    }

    pub fn update_one_by_id(&mut self, col_spec: &CollectionSpecification, query: Option<&Document>, update: &Document, session_id: Option<&ObjectId>) -> DbResult<usize> {
        let session = self.get_session_by_id(session_id)?.clone();
        self.update_one(col_spec, query, update, session.clone())
    }

    pub fn update_one(&mut self, col_spec: &CollectionSpecification, query: Option<&Document>, update: &Document, session: Arc<Mutex<SessionInner>>) -> DbResult<usize> {
        let result = self.internal_update(session, col_spec, query, update, false)?;

        Ok(result)
    }

    fn internal_update(&self, session: Arc<Mutex<SessionInner>>, col_spec: &CollectionSpecification, query: Option<&Document>, update: &Document, is_many: bool) -> DbResult<usize> {
        let subprogram = SubProgram::compile_update(
            col_spec,
            query,
            update,
            true,
            is_many,
        )?;

        let mut vm = VM::new(self.kv_engine.clone(), session, subprogram);
        vm.execute()?;

        Ok(vm.r2 as usize)
    }

    pub fn drop_collection_by_id(&mut self, name: &str, session_id: Option<&ObjectId>) -> DbResult<()> {
        let session_ref = self.get_session_by_id(session_id)?.clone();
        let mut session = session_ref.lock()?;
        self.drop_collection(name, &mut session)
    }

    pub fn drop_collection(&mut self, name: &str, session: &mut SessionInner) -> DbResult<()> {
        self.auto_start_transaction(session, TransactionType::Write)?;

        try_db_op!(self, session, self.internal_drop(session, name));

        Ok(())
    }

    fn internal_drop(&self, session: &mut SessionInner, name: &str) -> DbResult<()> {
        unimplemented!()
        // let meta_source = DbContext::get_meta_source(session)?;
        // let collection_meta = DbContext::internal_get_collection_id_by_name(session, name)?;
        // delete_all_helper::delete_all(session, &collection_meta)?;
        //
        // let mut btree_wrapper = BTreePageDeleteWrapper::new(
        //     session, meta_source.meta_pid);
        //
        // let pkey = Bson::from(name);
        // btree_wrapper.delete_item(&pkey)?;
        //
        // DbContext::update_meta_source(session, &meta_source)
    }

    pub fn delete_by_id(&mut self, col_name: &str, query: Document, is_many: bool, session_id: Option<&ObjectId>) -> DbResult<usize> {
        let session = self.get_session_by_id(session_id)?.clone();
        self.delete(col_name, query, is_many, session.clone())
    }

    pub fn delete(&mut self, col_name: &str, query: Document, is_many: bool, session: Arc<Mutex<SessionInner>>) -> DbResult<usize> {
        {
            let mut session = session.lock()?;
            self.auto_start_transaction(&mut session, TransactionType::Write)?;
        }

        let result = match self.internal_delete_by_query(session.clone(), col_name, query, is_many) {
            Ok(result) => {
                let mut session = session.lock()?;
                self.auto_commit(&mut session)?;
                result
            }
            Err(err) => {
                let mut session = session.lock()?;
                self.auto_rollback(&mut session)?;
                return Err(err);
            }
        };

        Ok(result)
    }

    // fn internal_delete(&self, session: &mut SessionInner, col_name: &str, primary_keys: &[Bson]) -> DbResult<usize> {
    //     let mut count: usize = 0;
    //     for pkey in primary_keys {
    //         let delete_result = self.internal_delete_by_pkey(session, col_name, pkey)?;
    //         if delete_result.is_some() {
    //             count += 1;
    //         }
    //     }
    //
    //     Ok(count)
    // }

    fn internal_delete_by_query(&mut self, session: Arc<Mutex<SessionInner>>, col_name: &str, query: Document, is_many: bool) -> DbResult<usize> {
        // let primary_keys = self.get_primary_keys_by_query(
        //     session,
        //     col_name,
        //     Some(query),
        //     is_many,
        // )?;
        // {
        //     let mut session = session.lock()?;
        //     self.internal_delete(&mut session, col_name, &primary_keys)
        // }
        unimplemented!()
    }

    fn internal_delete_all(&mut self, session: Arc<Mutex<SessionInner>>, col_name: &str) -> DbResult<usize> {
        // let primary_keys = self.get_primary_keys_by_query(
        //     session,
        //     col_name,
        //     None,
        //     true,
        // )?;
        // {
        //     let mut session = session.lock()?;
        //     self.internal_delete(&mut session, col_name, &primary_keys)
        // }
        unimplemented!()
    }

    pub fn delete_all_by_id(&mut self, col_name: &str, session_id: Option<&ObjectId>) -> DbResult<usize> {
        let session = self.get_session_by_id(session_id)?.clone();
        self.delete_all(col_name, session.clone())
    }

    pub fn delete_all(&mut self, col_name: &str, session: Arc<Mutex<SessionInner>>) -> DbResult<usize> {
        {
            let mut session = session.lock()?;
            self.auto_start_transaction(&mut session, TransactionType::Write)?;
        }

        let result = match self.internal_delete_all(session.clone(), col_name) {
            Ok(result) => {
                let mut session = session.lock()?;
                self.auto_commit(&mut session)?;
                result
            },
            Err(err) => {
                let mut session = session.lock()?;
                self.auto_rollback(&mut session)?;
                return Err(err);
            }
        };

        Ok(result)
    }

    fn get_primary_keys_by_query(&mut self, session: Arc<Mutex<SessionInner>>, col_name: &str, query: Option<Document>, is_many: bool) -> DbResult<Vec<Bson>> {
        let col_spec = {
            let session = session.lock()?;
            self.internal_get_collection_id_by_name(&session, col_name)?
        };
        let mut handle = self.find_internal(session, &col_spec, query)?;
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

    // fn update_by_root_pid(&self, session_id: Option<&ObjectId>, prefix: Bson, key: &Bson, doc: &Document) -> DbResult<bool> {
    //     let kv_id = self.find_kv_id_by_session_id(session_id);
    //
    //     let kv_cursor = self.kv_engine.open_multi_cursor(kv_id);
    //
    //     let mut cursor = Cursor::new(prefix, kv_cursor);
    //
    //     let reset_result = cursor.reset_by_pkey(key)?;
    //
    //     if !reset_result {
    //         return Ok(false);
    //     }
    //
    //     cursor.update_current(doc)?;
    //
    //     Ok(true)
    // }

    #[allow(dead_code)]
    pub(crate) fn delete_by_pkey(&mut self, col_name: &str, key: &Bson, session: Arc<Mutex<SessionInner>>) -> DbResult<Option<Document>> {
        {
            let mut session = session.lock()?;
            self.auto_start_transaction(&mut session, TransactionType::Write)?;
        }

        let result = match self.internal_delete_by_pkey(session.clone(), col_name, key) {
            Ok(result) => {
                let mut session = session.lock()?;
                self.auto_commit(&mut session)?;
                result
            },
            Err(err) => {
                let mut session = session.lock()?;
                self.auto_rollback(&mut session)?;
                return Err(err);
            },
        };

        Ok(result)
    }

    fn internal_delete_by_pkey(&self, _session: Arc<Mutex<SessionInner>>, col_name: &str, key: &Bson) -> DbResult<Option<Document>> {
        // let collection_meta = self.internal_get_collection_id_by_name(
        //     session_id, col_name,
        // )?;

        unimplemented!();
    }

    pub fn count(&mut self, name: &str, session_id: Option<&ObjectId>) -> DbResult<u64> {
        unimplemented!()
    }

    pub(crate) fn query_all_meta(&mut self, session_id: Option<&ObjectId>) -> DbResult<Vec<Document>> {
        unimplemented!()
    }

    pub fn start_transaction(&mut self, ty: Option<TransactionType>, session_id: Option<&ObjectId>) -> DbResult<()> {
        unimplemented!()
    }

    pub fn commit(&mut self, session_id: Option<&ObjectId>) -> DbResult<()> {
        unimplemented!()
    }

    pub fn rollback(&mut self, session_id: Option<&ObjectId>) -> DbResult<()> {
        unimplemented!()
    }

    pub fn drop_session(&mut self, session_id: &ObjectId) -> DbResult<()> {
        unimplemented!()
    }

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
        // TODO: FIXME
        // if !self.base_session.transaction_state().is_no_trans() {
        //     let _ = self.base_session.only_rollback_journal();
        // }
    }

}
