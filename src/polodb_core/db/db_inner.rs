/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::borrow::Borrow;
use std::collections::HashMap;
use std::io::Read;
use bson::{Binary, Bson, DateTime, Document};
use serde::Serialize;
use super::db::DbResult;
use crate::error::DbErr;
use crate::{LsmKv, TransactionType};
use crate::Config;
use crate::vm::SubProgram;
use crate::meta_doc_helper::meta_doc_key;
// use crate::index_ctx::{IndexCtx, merge_options_into_default};
use crate::page::RawPage;
use crate::db::db_handle::DbHandle;
use crate::results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;
#[cfg(target_arch = "wasm32")]
use crate::backend::indexeddb::IndexedDbBackend;
use bson::oid::ObjectId;
use bson::spec::BinarySubtype;
use serde::de::DeserializeOwned;
use crate::collection_info::{CollectionSpecification, CollectionSpecificationInfo, CollectionType};
use crate::cursor::Cursor;
use crate::metrics::Metrics;
use crate::session::SessionInner;
use crate::vm::VM;

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

const TABLE_META_PREFIX: &'static str = "$TABLE_META";

/**
 * API for all platforms
 */
pub(crate) struct DatabaseInner {
    kv_engine:    LsmKv,
    node_id:      [u8; 6],
    metrics:      Metrics,
    #[allow(dead_code)]
    config:       Config,
}

#[derive(Debug, Clone, Copy)]
pub struct MetaSource {
    pub meta_pid: u32,
}

impl DatabaseInner {

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_file(path: &Path, config: Config) -> DbResult<DatabaseInner> {
        let metrics = Metrics::new();
        let kv_engine = LsmKv::open_file(path)?;

        DatabaseInner::open_with_backend(
            kv_engine,
            config,
            metrics,
        )
    }

    #[cfg(target_arch = "wasm32")]
    pub fn open_indexeddb(ctx: crate::IndexedDbContext, config: Config) -> DbResult<DatabaseInner> {
        let metrics = Metrics::new();
        let page_size = NonZeroU32::new(4096).unwrap();
        let config = Arc::new(config);
        let backend = Box::new(IndexedDbBackend::open(
            ctx, page_size, config.init_block_count
        ));
        DatabaseInner::open_with_backend(backend, page_size, config, metrics)
    }

    pub fn open_memory(config: Config) -> DbResult<DatabaseInner> {
        let metrics = Metrics::new();
        let kv_engine = LsmKv::open_memory()?;
        DatabaseInner::open_with_backend(
            kv_engine,
            config,
            metrics,
        )
    }

    fn open_with_backend(
        kv_engine: LsmKv,
        config: Config,
        metrics: Metrics,
    ) -> DbResult<DatabaseInner> {
        let mut node_id: [u8; 6] = [0; 6];
        getrandom::getrandom(&mut node_id).unwrap();

        let ctx = DatabaseInner {
            kv_engine,
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

    pub fn start_session(&mut self) -> DbResult<SessionInner> {
        let kv_session = self.kv_engine.new_session();
        let inner = SessionInner::new(kv_session);
        Ok(inner)
    }

    fn internal_get_collection_id_by_name(&self, session: &SessionInner, name: &str) -> DbResult<CollectionSpecification> {
        let mut cursor =  {
            let kv_cursor = self.kv_engine.open_multi_cursor(Some(session.kv_session()));
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
            DatabaseInner::get_collection_meta_by_name_advanced(self, session, name, create_if_not_exist, &self.node_id)
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
        session.auto_start_transaction(ty)
    }

    fn auto_commit(&self, session: &mut SessionInner) -> DbResult<()> {
        session.auto_commit()?;
        Ok(())
    }

    fn auto_rollback(&self, session: &mut SessionInner) -> DbResult<()> {
        session.auto_rollback()?;
        Ok(())
    }

    pub fn create_collection(&mut self, name: &str) -> DbResult<CollectionSpecification> {
        let mut session = self.start_session()?;
        self.create_collection_internal(name, &mut session)
    }

    pub fn create_collection_internal(&mut self, name: &str, session: &mut SessionInner) -> DbResult<CollectionSpecification> {
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

        session.put(stacked_key.as_slice(), buffer.as_ref())?;

        Ok(spec)
    }

    pub(crate) fn make_handle<'a>(&self, session: &'a mut SessionInner, program: SubProgram) -> DbResult<DbHandle<'a>> {
        let vm = VM::new(self.kv_engine.clone(), session, program);
        Ok(DbHandle::new(vm))
    }

    #[allow(dead_code)]
    pub fn create_index(&mut self, prefix: Bson, keys: &Document, options: Option<&Document>, session: &mut SessionInner) -> DbResult<()> {
        self.auto_start_transaction(session, TransactionType::Write)?;

        try_db_op!(self, session, DatabaseInner::internal_create_index(session, prefix, keys, options));

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

    pub fn insert_one(&mut self, col_name: &str, doc: Document, session: &mut SessionInner) -> DbResult<InsertOneResult> {
        self.auto_start_transaction(session, TransactionType::Write)?;

        let changed = try_db_op!(self, session, self.insert_one_internal(session, col_name, doc, &self.node_id));

        Ok(changed)
    }

    fn insert_one_internal(&self, session: &mut SessionInner, col_name: &str, doc: Document, node_id: &[u8; 6]) -> DbResult<InsertOneResult> {
        let col_meta = self.get_collection_meta_by_name_advanced(session, col_name, true, node_id)?
            .expect("internal: meta must exist");
        let (result, _) = self.insert_one_with_meta(session, col_meta, doc)?;
        Ok(result)
    }

    /// Insert one item with the collection spec
    /// return the new spec for the outside to do the following operation
    fn insert_one_with_meta(&self, session: &mut SessionInner, col_spec: CollectionSpecification, doc: Document) -> DbResult<(InsertOneResult, CollectionSpecification)> {
        let doc  = DatabaseInner::fix_doc(doc);

        let pkey = doc.get("_id").unwrap();

        let stacked_key = crate::utils::bson::stacked_key([
            &Bson::String(col_spec._id.clone()),
            &pkey,
        ])?;

        let doc_buf = bson::to_vec(&doc)?;

        session.put(
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

    pub fn insert_many<T: Serialize>(
        &mut self,
        col_name: &str,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        session: &mut SessionInner
    ) -> DbResult<InsertManyResult> {
        self.auto_start_transaction(session, TransactionType::Write)?;

        let result = try_db_op!(self, session, self.insert_many_internal(session, col_name, docs, &self.node_id));

        Ok(result)
    }

    fn insert_many_internal<T: Serialize>(
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

    fn find_internal<'b>(
        &self,
        session: &'b mut SessionInner,
        col_spec: &CollectionSpecification,
        query: Option<Document>,
    ) -> DbResult<DbHandle<'b>> {
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

    pub fn update_one(
        &mut self,
        col_name: &str,
        query: Option<&Document>,
        update: &Document,
        session: &mut SessionInner,
    ) -> DbResult<UpdateResult> {
        session.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(self, session, self.internal_update(col_name, query, update, false, session));

        Ok(result)
    }

    pub(super) fn update_many(
        &mut self,
        col_name: &str,
        query: Document,
        update: Document,
        session: &mut SessionInner,
    ) -> DbResult<UpdateResult> {
        session.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(self, session, self.internal_update(col_name, Some(&query), &update, true, session));

        Ok(result)
    }

    fn internal_update(
        &mut self,
        col_name: &str,
        query: Option<&Document>,
        update: &Document,
        is_many: bool,
        session: &mut SessionInner,
    ) -> DbResult<UpdateResult> {
        let meta_opt = self.get_collection_meta_by_name_advanced_auto(col_name, false, session)?;

        let modified_count = match &meta_opt {
            Some(col_spec) => {
                let subprogram = SubProgram::compile_update(
                    col_spec,
                    query,
                    update,
                    true,
                    is_many,
                )?;

                let mut vm = VM::new(self.kv_engine.clone(), session, subprogram);
                vm.set_rollback_on_drop(true);
                vm.execute()?;

                vm.r2 as u64
            },
            None => 0,
        };

        Ok(UpdateResult {
            modified_count,
        })
    }

    pub fn drop_collection(&mut self, col_name: &str, session: &mut SessionInner) -> DbResult<()> {
        self.auto_start_transaction(session, TransactionType::Write)?;

        try_db_op!(self, session, self.drop_collection_internal(col_name, session));

        Ok(())
    }

    fn drop_collection_internal(&mut self, col_name: &str, session: &mut SessionInner) -> DbResult<()> {
        // Delete content begin
        let subprogram = SubProgram::compile_delete_all(
            col_name,
            true,
        )?;

        {
            let mut vm = VM::new(self.kv_engine.clone(), session, subprogram);
            vm.set_rollback_on_drop(true);
            vm.execute()?;
        } // Delete content end

        self.delete_collection_meta(col_name, session)?;

        Ok(())
    }

    fn delete_collection_meta(&mut self, col_name: &str, session: &mut SessionInner) -> DbResult<()> {
        let mut cursor = {
            let multi_cursor = self.kv_engine.open_multi_cursor(Some(session.kv_session()));
            Cursor::new(TABLE_META_PREFIX, multi_cursor)
        };

        let found = cursor.reset_by_pkey(&col_name.into())?;
        if found {
            session.delete_cursor_current(cursor.multi_cursor_mut())?;
        }

        Ok(())
    }

    pub fn delete(&mut self, col_name: &str, query: Document, is_many: bool, session: &mut SessionInner) -> DbResult<usize> {
        let result = self.internal_delete_by_query(session, col_name, query, is_many)?;
        Ok(result)
    }

    fn internal_delete_by_query(&mut self, session: &mut SessionInner, col_name: &str, query: Document, is_many: bool) -> DbResult<usize> {
        let subprogram = SubProgram::compile_delete(
            col_name,
            Some(&query),
            true,
            is_many,
        )?;

        let mut vm = VM::new(self.kv_engine.clone(), session, subprogram);
        vm.set_rollback_on_drop(true);
        vm.execute()?;

        Ok(vm.r2 as usize)
    }

    fn internal_delete_all(&mut self, session: &mut SessionInner, col_name: &str) -> DbResult<usize> {
        // Delete content begin
        let subprogram = SubProgram::compile_delete_all(
            col_name,
            true,
        )?;

        let delete_count = {
            let mut vm = VM::new(self.kv_engine.clone(), session, subprogram);
            vm.set_rollback_on_drop(true);
            vm.execute()?;

            vm.r2 as usize
        }; // Delete content end

        Ok(delete_count)
    }

    pub fn delete_all(&mut self, col_name: &str, session: &mut SessionInner) -> DbResult<usize> {
        self.auto_start_transaction(session, TransactionType::Write)?;

        let result = try_db_op!(
            self,
            session,
            self.internal_delete_all(session, col_name)
        );

        Ok(result)
    }

    // fn get_primary_keys_by_query(&mut self, session: &mut SessionInner, col_name: &str, query: Option<Document>, is_many: bool) -> DbResult<Vec<Bson>> {
    //     let col_spec = self.internal_get_collection_id_by_name(session, col_name)?;
    //     let mut handle = self.find_internal(session, &col_spec, query)?;
    //     let mut buffer: Vec<Bson> = vec![];
    //
    //     handle.step()?;
    //
    //     while handle.has_row() {
    //         let doc = handle.get().as_document().unwrap();
    //         let pkey = doc.get("_id").unwrap();
    //         buffer.push(pkey.clone());
    //
    //         if !is_many {
    //             handle.commit_and_close_vm()?;
    //             return Ok(buffer);
    //         }
    //
    //         handle.step()?;
    //     }
    //
    //     handle.commit_and_close_vm()?;
    //     Ok(buffer)
    // }

    pub fn count(&mut self, name: &str, session: &mut SessionInner) -> DbResult<u64> {
        let col = self.get_collection_meta_by_name_advanced_auto(
            name,
            false,
            session,
        )?;
        if col.is_none() {
            return Ok(0);
        }

        let col = col.unwrap();
        let mut count = 0;

        let mut handle = self.find_internal(session, &col, None)?;
        handle.step()?;

        while handle.has_row() {
            count += 1;

            handle.step()?;
        }

        handle.commit_and_close_vm()?;

        Ok(count)
    }

    pub(crate) fn list_collection_names_with_session(&mut self, session: &mut SessionInner) -> DbResult<Vec<String>> {
        let docs = self.query_all_meta(session)?;
        Ok(collection_metas_to_names(docs))
    }

    pub(crate) fn query_all_meta(&mut self, session: &mut SessionInner) -> DbResult<Vec<Document>> {
        let mut handle = {
            let subprogram = SubProgram::compile_query_all_by_name(
                TABLE_META_PREFIX,
                true
            )?;

            self.make_handle(session, subprogram)?
        };

        handle.step()?;

        let mut result = Vec::new();

        while handle.has_row() {
            let value = handle.get();
            result.push(value.as_document().unwrap().clone());

            handle.step()?;
        }

        Ok(result)
    }

    pub fn handle_request<R: Read>(&mut self, pipe_in: &mut R) -> DbResult<HandleRequestResult> {
        unimplemented!()
    }

    pub fn handle_request_doc(&mut self, value: Bson) -> DbResult<HandleRequestResult> {
        unimplemented!()
    }

    pub fn find_one<T: DeserializeOwned>(
        &mut self,
        col_name: &str,
        filter: impl Into<Option<Document>>,
        session: &mut SessionInner,
    ) -> DbResult<Option<T>> {
        let filter_query = filter.into();
        let col_spec = self.get_collection_meta_by_name_advanced_auto(col_name, false, session)?;
        let result: Option<T> = if let Some(col_spec) = col_spec {
            let mut handle = self.find_internal(
                session,
                &col_spec,
                filter_query,
            )?;
            handle.step()?;

            if !handle.has_row() {
                handle.commit_and_close_vm()?;
                return Ok(None);
            }

            let result_doc = handle.get().as_document().unwrap().clone();

            handle.commit_and_close_vm()?;

            bson::from_document(result_doc)?
        } else {
            None
        };

        Ok(result)
    }

    pub fn find_many<T: DeserializeOwned>(
        &mut self,
        col_name: &str,
        filter: impl Into<Option<Document>>,
        session: &mut SessionInner
    ) -> DbResult<Vec<T>> {
        let filter_query = filter.into();
        let meta_opt = self.get_collection_meta_by_name_advanced_auto(col_name, false, session)?;
        match meta_opt {
            Some(col_spec) => {
                let mut handle = self.find_internal(
                    session,
                    &col_spec,
                    filter_query,
                )?;

                let mut result: Vec<T> = Vec::new();
                consume_handle_to_vec::<T>(&mut handle, &mut result)?;

                Ok(result)

            }
            None => {
                Ok(vec![])
            }
        }
    }

    pub(super) fn count_documents(&mut self, col_name: &str, session: &mut SessionInner) -> DbResult<u64> {
        let test_result = self.count(col_name, session);
        match test_result {
            Ok(result) => Ok(result),
            Err(DbErr::CollectionNotFound(_)) => Ok(0),
            Err(err) => Err(err),
        }
    }

    pub(super) fn delete_one(
        &mut self,
        col_name: &str,
        query: Document,
        session: &mut SessionInner,
    ) -> DbResult<DeleteResult> {
        let test_count = self.delete(
            col_name,
            query,
            false,
            session,
        );

        match test_count {
            Ok(count) => Ok(DeleteResult {
                deleted_count: count as u64,
            }),
            Err(DbErr::CollectionNotFound(_)) => Ok(DeleteResult {
                deleted_count: 0,
            }),
            Err(err) => Err(err),
        }
    }

    pub(super) fn delete_many(&mut self, col_name: &str, query: Document, session: &mut SessionInner) -> DbResult<DeleteResult> {
        let test_deleted_count = if query.len() == 0 {
            self.delete_all(col_name, session)
        } else {
            self.delete(col_name, query, true, session)
        };
        match test_deleted_count {
            Ok(deleted_count) => Ok(DeleteResult {
                deleted_count: deleted_count as u64,
            }),
            Err(DbErr::CollectionNotFound(_)) => Ok(DeleteResult {
                deleted_count: 0
            }),
            Err(err) => Err(err),
        }
    }

}

#[derive(Clone)]
pub struct HandleRequestResult {
    pub is_quit: bool,
    pub value: Bson,
}

fn consume_handle_to_vec<T: DeserializeOwned>(handle: &mut DbHandle, result: &mut Vec<T>) -> DbResult<()> {
    handle.step()?;

    while handle.has_row() {
        let doc_result = handle.get().as_document().unwrap();
        let item: T = bson::from_document(doc_result.clone())?;
        result.push(item);

        handle.step()?;
    }

    Ok(())
}

fn collection_metas_to_names(doc_meta: Vec<Document>) -> Vec<String> {
    doc_meta
        .iter()
        .map(|doc| {
            let name = doc.get("_id").unwrap().as_str().unwrap().to_string();
            name
        })
        .collect()
}
