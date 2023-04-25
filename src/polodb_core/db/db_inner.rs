/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::borrow::Borrow;
use std::collections::HashMap;
use bson::{Binary, Bson, Document};
use serde::Serialize;
use super::db::Result;
use crate::errors::Error;
use crate::{ClientSessionCursor, LsmKv, TransactionType};
use crate::Config;
use crate::vm::SubProgram;
use crate::meta_doc_helper::meta_doc_key;
// use crate::index_ctx::{IndexCtx, merge_options_into_default};
use crate::db::client_cursor::ClientCursor;
use crate::results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::JsValue;
use bson::oid::ObjectId;
use bson::spec::BinarySubtype;
use serde::de::DeserializeOwned;
use crate::collection_info::{CollectionSpecification, CollectionSpecificationInfo, CollectionType, IndexInfo};
use crate::cursor::Cursor;
use crate::metrics::Metrics;
use crate::session::SessionInner;
use crate::utils::bson::bson_datetime_now;
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
    pub fn open_file(path: &Path, config: Config) -> Result<DatabaseInner> {
        let metrics = Metrics::new();
        let kv_engine = LsmKv::open_file(path)?;

        DatabaseInner::open_with_backend(
            kv_engine,
            config,
            metrics,
        )
    }

    #[cfg(target_arch = "wasm32")]
    pub fn open_indexeddb(init_data: JsValue, config: Config) -> Result<DatabaseInner> {
        let metrics = Metrics::new();
        let kv_engine = LsmKv::open_indexeddb(init_data)?;

        DatabaseInner::open_with_backend(
            kv_engine,
            config,
            metrics,
        )
    }

    pub fn open_memory(config: Config) -> Result<DatabaseInner> {
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
    ) -> Result<DatabaseInner> {
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

    pub fn start_session(&self) -> Result<SessionInner> {
        let kv_session = self.kv_engine.new_session();
        let inner = SessionInner::new(kv_session);
        Ok(inner)
    }

    fn internal_get_collection_id_by_name(&self, session: &SessionInner, name: &str) -> Result<CollectionSpecification> {
        let mut cursor =  {
            let kv_cursor = self.kv_engine.open_multi_cursor(Some(session.kv_session()));
            Cursor::new(TABLE_META_PREFIX.to_string(), kv_cursor)
        };

        let key = Bson::from(name);

        let reset_result = cursor.reset_by_pkey(&key)?;
        if !reset_result {
            return Err(Error::CollectionNotFound(name.to_string()));
        }

        let data = cursor.peek_data(self.kv_engine.inner.as_ref())?;
        if data.is_none() {
            return Err(Error::CollectionNotFound(name.to_string()));
        }

        let entry = bson::from_slice::<CollectionSpecification>(data.unwrap().as_ref())?;
        Ok(entry)
    }

    pub fn get_collection_meta_by_name_advanced_auto(
        &self,
        name: &str,
        create_if_not_exist: bool,
        session: &mut SessionInner,
    ) -> Result<Option<CollectionSpecification>> {
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

    pub fn get_collection_meta_by_name_advanced(&self, session: &mut SessionInner, name: &str, create_if_not_exist: bool, node_id: &[u8; 6]) -> Result<Option<CollectionSpecification>> {
        match self.internal_get_collection_id_by_name(session, name) {
            Ok(meta) => Ok(Some(meta)),
            Err(Error::CollectionNotFound(_)) => {
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

    fn auto_start_transaction(&self, session: &mut SessionInner, ty: TransactionType) -> Result<()> {
        session.auto_start_transaction(ty)
    }

    fn auto_commit(&self, session: &mut SessionInner) -> Result<()> {
        session.auto_commit()?;
        Ok(())
    }

    fn auto_rollback(&self, session: &mut SessionInner) -> Result<()> {
        session.auto_rollback()?;
        Ok(())
    }

    pub fn create_collection(&self, name: &str) -> Result<CollectionSpecification> {
        DatabaseInner::validate_col_name(name)?;

        let mut session = self.start_session()?;
        self.create_collection_internal(name, &mut session)
    }

    pub fn create_collection_internal(&self, name: &str, session: &mut SessionInner) -> Result<CollectionSpecification> {
        self.auto_start_transaction(session, TransactionType::Write)?;

        let meta = try_db_op!(self, session, self.internal_create_collection(session, name, &self.node_id));

        Ok(meta)
    }

    fn check_collection_exist(&self, session: &mut SessionInner, name: &str) -> Result<bool> {
        let test_collection = self.internal_get_collection_id_by_name(session, name);
        match test_collection {
            Ok(_) => Ok(true),
            Err(Error::CollectionNotFound(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn internal_create_collection(&self, session: &mut SessionInner, name: &str, node_id: &[u8; 6]) -> Result<CollectionSpecification> {
        if name.is_empty() {
            return Err(Error::IllegalCollectionName(name.into()));
        }
        let exist = self.check_collection_exist(session, name)?;
        if exist {
            return Err(Error::CollectionAlreadyExits(name.into()));
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

                create_at: bson_datetime_now(),
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

    pub(crate) fn make_handle<T: DeserializeOwned>(&self, program: SubProgram) -> Result<ClientSessionCursor<T>> {
        let vm = VM::new(self.kv_engine.clone(), program);
        Ok(ClientSessionCursor::new(vm))
    }

    pub fn create_index(&self, col_name: &str, keys: Document, options: impl Into<Option<Document>>, session: &mut SessionInner) -> Result<()> {
        DatabaseInner::validate_col_name(col_name)?;

        self.auto_start_transaction(session, TransactionType::Write)?;

        try_db_op!(self, session, self.internal_create_index(session, col_name, keys, options));

        Ok(())
    }

    fn internal_create_index(&self, session: &mut SessionInner, col_name: &str, keys: Document, options: impl Into<Option<Document>>) -> Result<()> {
        let options = options.into();

        if keys.len() != 1 {
            return Err(Error::OnlySupportSingleFieldIndexes(Box::new(keys)));
        }

        let tuples = keys.iter().collect::<Vec<(&String, &Bson)>>();
        let first_tuple = tuples.first().unwrap();

        let (key, value) = first_tuple;

        self.create_single_index(session, col_name, key.as_str(), value, options)
    }

    fn create_single_index(&self, session: &mut SessionInner, col_name: &str, key: &str, order: &Bson, options: Option<Document>) -> Result<()> {
        if !DatabaseInner::is_num_1(order) {
            return Err(Error::OnlySupportsAscendingOrder(key.to_string()));
        }

        let index_name = DatabaseInner::make_index_name(key, 1, &options)?;

        let mut collection_spec = self.internal_get_collection_id_by_name(session, col_name)?;

        if collection_spec.indexes.get(&index_name).is_some() {
            return Ok(())
        }

        let index_info = IndexInfo::single_index(key.to_string(), 1);
        collection_spec.indexes.insert(index_name, index_info);

        let stacked_key = crate::utils::bson::stacked_key(&[
            Bson::String(TABLE_META_PREFIX.to_string()),
            Bson::String(col_name.to_string()),
        ])?;

        let buffer = bson::to_vec(&collection_spec)?;

        session.put(stacked_key.as_slice(), buffer.as_ref())?;

        Ok(())
    }

    fn make_index_name(key: &str, order: i32, index_options: &Option<Document>) -> Result<String> {
        if let Some(doc) = index_options {
            let name = doc.get("name");
            if let Some(Bson::String(name)) = name {
                DatabaseInner::validate_index_name(name)?;
                return Ok(name.clone());
            }
        }

        let mut index_name = key.to_string();

        index_name += "_";
        let num_str = order.to_string();
        index_name += &num_str;

        Ok(index_name)
    }

    #[inline]
    fn is_num_1(val: &Bson) -> bool {
        match val {
            Bson::Int32(1) => true,
            Bson::Int64(1) => true,
            _ => false,
        }
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

    fn validate_col_name(col_name: &str) -> Result<()> {
        for ch in col_name.chars() {
            if ch == '$' || ch == '\n' || ch == '\t' || ch == '\r' {
                return Err(Error::IllegalCollectionName(col_name.to_string()))
            }
        }

        Ok(())
    }

    fn validate_index_name(col_name: &str) -> Result<()> {
        for ch in col_name.chars() {
            if ch == '$' || ch == '\n' || ch == '\t' || ch == '\r' {
                return Err(Error::IllegalIndexName(col_name.to_string()))
            }
        }

        Ok(())
    }

    pub fn insert_one(&self, col_name: &str, doc: Document, session: &mut SessionInner) -> Result<InsertOneResult> {
        DatabaseInner::validate_col_name(col_name)?;

        self.auto_start_transaction(session, TransactionType::Write)?;

        let changed = try_db_op!(self, session, self.insert_one_internal(session, col_name, doc, &self.node_id));

        Ok(changed)
    }

    fn insert_one_internal(&self, session: &mut SessionInner, col_name: &str, doc: Document, node_id: &[u8; 6]) -> Result<InsertOneResult> {
        let col_meta = self.get_collection_meta_by_name_advanced(session, col_name, true, node_id)?
            .expect("internal: meta must exist");
        let (result, _) = self.insert_one_with_meta(session, col_meta, doc)?;
        Ok(result)
    }

    /// Insert one item with the collection spec
    /// return the new spec for the outside to do the following operation
    fn insert_one_with_meta(&self, session: &mut SessionInner, col_spec: CollectionSpecification, doc: Document) -> Result<(InsertOneResult, CollectionSpecification)> {
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
        &self,
        col_name: &str,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        session: &mut SessionInner
    ) -> Result<InsertManyResult> {
        DatabaseInner::validate_col_name(col_name)?;

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
    ) -> Result<InsertManyResult> {
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

    fn find_internal<T: DeserializeOwned>(
        &self,
        col_spec: &CollectionSpecification,
        query: Option<Document>,
    ) -> Result<ClientSessionCursor<T>> {
        let subprogram = match query {
            Some(query) => SubProgram::compile_query(
                col_spec,
                &query,
                true
            ),
            None => SubProgram::compile_query_all(col_spec, true),
        }?;

        let handle = self.make_handle(subprogram)?;
        Ok(handle)
    }

    pub fn update_one(
        &self,
        col_name: &str,
        query: Option<&Document>,
        update: &Document,
        session: &mut SessionInner,
    ) -> Result<UpdateResult> {
        DatabaseInner::validate_col_name(col_name)?;
        session.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(self, session, self.internal_update(col_name, query, update, false, session));

        Ok(result)
    }

    pub(super) fn update_many(
        &self,
        col_name: &str,
        query: Document,
        update: Document,
        session: &mut SessionInner,
    ) -> Result<UpdateResult> {
        DatabaseInner::validate_col_name(col_name)?;
        session.auto_start_transaction(TransactionType::Write)?;

        let result = try_db_op!(self, session, self.internal_update(col_name, Some(&query), &update, true, session));

        Ok(result)
    }

    fn internal_update(
        &self,
        col_name: &str,
        query: Option<&Document>,
        update: &Document,
        is_many: bool,
        session: &mut SessionInner,
    ) -> Result<UpdateResult> {
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

                let mut vm = VM::new(self.kv_engine.clone(), subprogram);
                vm.execute(session)?;

                vm.r2 as u64
            },
            None => 0,
        };

        Ok(UpdateResult {
            modified_count,
        })
    }

    pub fn drop_collection(&self, col_name: &str, session: &mut SessionInner) -> Result<()> {
        DatabaseInner::validate_col_name(col_name)?;

        self.auto_start_transaction(session, TransactionType::Write)?;

        try_db_op!(self, session, self.drop_collection_internal(col_name, session));

        Ok(())
    }

    fn drop_collection_internal(&self, col_name: &str, session: &mut SessionInner) -> Result<()> {
        // Delete content begin
        let subprogram = SubProgram::compile_delete_all(
            col_name,
            true,
        )?;

        {
            let mut vm = VM::new(self.kv_engine.clone(), subprogram);
            vm.execute(session)?;
        } // Delete content end

        self.delete_collection_meta(col_name, session)?;

        Ok(())
    }

    fn delete_collection_meta(&self, col_name: &str, session: &mut SessionInner) -> Result<()> {
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

    pub fn delete(&self, col_name: &str, query: Document, is_many: bool, session: &mut SessionInner) -> Result<usize> {
        let result = self.internal_delete_by_query(session, col_name, query, is_many)?;
        Ok(result)
    }

    fn internal_delete_by_query(&self, session: &mut SessionInner, col_name: &str, query: Document, is_many: bool) -> Result<usize> {
        let subprogram = SubProgram::compile_delete(
            col_name,
            Some(&query),
            true,
            is_many,
        )?;

        let mut vm = VM::new(self.kv_engine.clone(), subprogram);
        vm.execute(session)?;

        Ok(vm.r2 as usize)
    }

    fn internal_delete_all(&self, session: &mut SessionInner, col_name: &str) -> Result<usize> {
        // Delete content begin
        let subprogram = SubProgram::compile_delete_all(
            col_name,
            true,
        )?;

        let delete_count = {
            let mut vm = VM::new(self.kv_engine.clone(), subprogram);
            vm.execute(session)?;

            vm.r2 as usize
        }; // Delete content end

        Ok(delete_count)
    }

    pub fn delete_all(&self, col_name: &str, session: &mut SessionInner) -> Result<usize> {
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

    pub fn count(&self, name: &str, session: &mut SessionInner) -> Result<u64> {
        DatabaseInner::validate_col_name(name)?;

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

        let mut handle = self.find_internal::<Document>(&col, None)?;

        while handle.advance_inner(session)? {
            count += 1;
        }

        Ok(count)
    }

    pub(crate) fn list_collection_names_with_session(&self, session: &mut SessionInner) -> Result<Vec<String>> {
        let docs = self.query_all_meta(session)?;
        Ok(collection_metas_to_names(docs))
    }

    pub(crate) fn query_all_meta(&self, session: &mut SessionInner) -> Result<Vec<Document>> {
        let mut handle: ClientSessionCursor<Document> = {
            let subprogram = SubProgram::compile_query_all_by_name(
                TABLE_META_PREFIX,
                true
            )?;

            self.make_handle(subprogram)?
        };


        let mut result = Vec::new();

        while handle.advance_inner(session)? {
            let value = handle.get();
            result.push(value.as_document().unwrap().clone());
        }

        Ok(result)
    }

    pub fn find_with_owned_session<T: DeserializeOwned>(
        &self,
        col_name: &str,
        filter: impl Into<Option<Document>>,
        mut session: SessionInner,
    ) -> Result<ClientCursor<T>> {
        DatabaseInner::validate_col_name(col_name)?;
        let filter_query = filter.into();
        let meta_opt = self.get_collection_meta_by_name_advanced_auto(col_name, false, &mut session)?;
        let subprogram = match meta_opt {
            Some(col_spec) => {
                let subprogram = match filter_query {
                    Some(query) => SubProgram::compile_query(
                        &col_spec,
                        &query,
                        true
                    ),
                    None => SubProgram::compile_query_all(&col_spec, true),
                }?;

                subprogram
            }
            None => SubProgram::compile_empty_query(),
        };

        let vm = VM::new(self.kv_engine.clone(), subprogram);

        let handle = ClientCursor::new(vm, session);

        Ok(handle)
    }

    pub fn find_with_borrowed_session<T: DeserializeOwned>(
        &self,
        col_name: &str,
        filter: impl Into<Option<Document>>,
        session: &mut SessionInner
    ) -> Result<ClientSessionCursor<T>> {
        DatabaseInner::validate_col_name(col_name)?;
        let filter_query = filter.into();
        let meta_opt = self.get_collection_meta_by_name_advanced_auto(col_name, false, session)?;
        match meta_opt {
            Some(col_spec) => {
                let handle = self.find_internal(
                    &col_spec,
                    filter_query,
                )?;

                Ok(handle)
            }
            None => {
                let subprogram = SubProgram::compile_empty_query();
                let vm = VM::new(self.kv_engine.clone(), subprogram);
                let cursor = ClientSessionCursor::new(vm);
                Ok(cursor)
            }
        }
    }

    pub(super) fn count_documents(&self, col_name: &str, session: &mut SessionInner) -> Result<u64> {
        DatabaseInner::validate_col_name(col_name)?;
        let test_result = self.count(col_name, session);
        match test_result {
            Ok(result) => Ok(result),
            Err(Error::CollectionNotFound(_)) => Ok(0),
            Err(err) => Err(err),
        }
    }

    pub(super) fn delete_one(
        &self,
        col_name: &str,
        query: Document,
        session: &mut SessionInner,
    ) -> Result<DeleteResult> {
        DatabaseInner::validate_col_name(col_name)?;

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
            Err(Error::CollectionNotFound(_)) => Ok(DeleteResult {
                deleted_count: 0,
            }),
            Err(err) => Err(err),
        }
    }

    pub(super) fn delete_many(&self, col_name: &str, query: Document, session: &mut SessionInner) -> Result<DeleteResult> {
        DatabaseInner::validate_col_name(col_name)?;

        let test_deleted_count = if query.len() == 0 {
            self.delete_all(col_name, session)
        } else {
            self.delete(col_name, query, true, session)
        };
        match test_deleted_count {
            Ok(deleted_count) => Ok(DeleteResult {
                deleted_count: deleted_count as u64,
            }),
            Err(Error::CollectionNotFound(_)) => Ok(DeleteResult {
                deleted_count: 0
            }),
            Err(err) => Err(err),
        }
    }

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

#[cfg(test)]
mod tests {
    use crate::db::db_inner::DatabaseInner;

    #[test]
    fn test_validate_col_name() {
        assert!(DatabaseInner::validate_col_name("test").is_ok());
        assert!(DatabaseInner::validate_col_name("$test$").is_err());
        assert!(DatabaseInner::validate_col_name("test\n").is_err());
    }

}
