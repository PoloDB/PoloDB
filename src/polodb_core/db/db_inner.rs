// Copyright 2024 Vincent Chan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Borrow;
use std::collections::HashMap;
use bson::{Bson, Document};
use serde::Serialize;
use super::db::Result;
use crate::errors::Error;
use crate::options::UpdateOptions;
use crate::Config;
use crate::vm::SubProgram;
use crate::meta_doc_helper::meta_doc_key;
use crate::index::{IndexBuilder, IndexModel, IndexOptions};
use crate::db::client_cursor::ClientCursor;
use crate::results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};
use std::path::Path;
use bson::oid::ObjectId;
use serde::de::DeserializeOwned;
use crate::coll::collection_info::{
    CollectionSpecification,
    IndexInfo,
};
use crate::cursor::Cursor;
use crate::index::{IndexHelper, IndexHelperOperation};
use crate::metrics::Metrics;
use crate::db::rocksdb_wrapper::RocksDBWrapper;
use crate::transaction::TransactionInner;
use crate::vm::VM;

const TABLE_META_PREFIX: &'static str = "$TABLE_META";

/**
 * API for all platforms
 */
pub(crate) struct DatabaseInner {
    rocksdb:      RocksDBWrapper,
    node_id:      [u8; 6],
    metrics:      Metrics,
    #[allow(dead_code)]
    config:       Config,
}

impl DatabaseInner {

    pub fn open_file(path: &Path, config: Config) -> Result<DatabaseInner> {
        let metrics = Metrics::new();

        DatabaseInner::open_with_backend(
            path,
            config,
            metrics,
        )
    }

    fn open_with_backend(
        path: &Path,
        config: Config,
        metrics: Metrics,
    ) -> Result<DatabaseInner> {
        let mut node_id: [u8; 6] = [0; 6];
        getrandom::getrandom(&mut node_id).unwrap();

        let rocksdb = RocksDBWrapper::open(path)?;

        let ctx = DatabaseInner {
            rocksdb,
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

    pub fn start_transaction(&self) -> Result<TransactionInner> {
        Ok(TransactionInner::new(self.rocksdb.begin_transaction()?))
    }

    fn internal_get_collection_id_by_name(&self, txn: &TransactionInner, name: &str) -> Result<CollectionSpecification> {
        let mut cursor =  {
            let kv_cursor = txn.rocksdb_txn.new_iterator();
            Cursor::new_with_str_prefix(TABLE_META_PREFIX.to_string(), kv_cursor)?
        };

        let key = Bson::from(name);

        let reset_result = cursor.reset_by_pkey(&key)?;

        if !reset_result {
            return Err(Error::CollectionNotFound(name.to_string()));
        }

        let data = cursor.copy_data()?;

        let entry = bson::from_slice::<CollectionSpecification>(data.as_slice())?;
        Ok(entry)
    }

    pub fn get_collection_meta_by_name_advanced_auto(
        &self,
        name: &str,
        create_if_not_exist: bool,
        txn: &TransactionInner,
    ) -> Result<Option<CollectionSpecification>> {
        let result = DatabaseInner::get_collection_meta_by_name_advanced(self, txn, name, create_if_not_exist, &self.node_id)?;

        Ok(result)
    }

    pub fn get_collection_meta_by_name_advanced(&self, txn: &TransactionInner, name: &str, create_if_not_exist: bool, node_id: &[u8; 6]) -> Result<Option<CollectionSpecification>> {
        match self.internal_get_collection_id_by_name(txn, name) {
            Ok(meta) => Ok(Some(meta)),
            Err(Error::CollectionNotFound(_)) => {
                if create_if_not_exist {
                    let meta = self.internal_create_collection(txn, name, node_id)?;
                    Ok(Some(meta))
                } else {
                    Ok(None)
                }
            },
            Err(err) => return Err(err),
        }
    }

    pub fn create_collection(&self, name: &str) -> Result<CollectionSpecification> {
        DatabaseInner::validate_col_name(name)?;

        let txn = self.start_transaction()?;
        let result = self.create_collection_internal(name, &txn)?;
        txn.commit()?;

        Ok(result)
    }

    #[inline]
    pub fn create_collection_internal(&self, name: &str, txn: &TransactionInner) -> Result<CollectionSpecification> {
        let meta = self.internal_create_collection(txn, name, &self.node_id)?;
        Ok(meta)
    }

    fn check_collection_exist(&self, txn: &TransactionInner, name: &str) -> Result<bool> {
        let test_collection = self.internal_get_collection_id_by_name(txn, name);
        match test_collection {
            Ok(_) => Ok(true),
            Err(Error::CollectionNotFound(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn internal_create_collection(&self, txn: &TransactionInner, name: &str, node_id: &[u8; 6]) -> Result<CollectionSpecification> {
        if name.is_empty() {
            return Err(Error::IllegalCollectionName(name.into()));
        }
        let exist = self.check_collection_exist(txn, name)?;
        if exist {
            return Err(Error::CollectionAlreadyExits(name.into()));
        }

        let uuid = uuid::Uuid::now_v1(node_id);
        let spec = CollectionSpecification::new(name.to_string(), uuid);

        let stacked_key = crate::utils::bson::stacked_key(&[
            Bson::String(TABLE_META_PREFIX.to_string()),
            Bson::String(name.to_string()),
        ])?;

        let buffer = bson::to_vec(&spec)?;

        txn.put(stacked_key.as_slice(), buffer.as_ref())?;

        Ok(spec)
    }

    pub(crate) fn make_handle<T: DeserializeOwned + Send + Sync>(&self, program: SubProgram, txn: TransactionInner) -> Result<ClientCursor<T>> {
        let vm = VM::new(
            txn,
            program,
            self.metrics.clone(),
        );
        Ok(ClientCursor::new(vm))
    }

    pub fn create_index(&self, col_name: &str, index: IndexModel, txn: &TransactionInner) -> Result<()> {
        DatabaseInner::validate_col_name(col_name)?;

        self.internal_create_index(txn, col_name, index)?;

        Ok(())
    }

    fn internal_create_index(&self, txn: &TransactionInner, col_name: &str, index: IndexModel) -> Result<()> {
        if index.keys.len() != 1 {
            return Err(Error::OnlySupportSingleFieldIndexes(Box::new(index.keys)));
        }

        let options = index.options.as_ref();

        let tuples = index.keys.iter().collect::<Vec<(&String, &Bson)>>();
        let first_tuple = tuples.first().unwrap();

        let (key, value) = first_tuple;

        self.create_single_index(txn, col_name, key.as_str(), value, options)
    }

    fn create_single_index(
        &self,
        txn: &TransactionInner,
        col_name: &str,
        key: &str,
        order: &Bson,
        options: Option<&IndexOptions>,
    ) -> Result<()> {
        if !DatabaseInner::is_num_1(order) {
            return Err(Error::OnlySupportsAscendingOrder(key.to_string()));
        }

        let index_name = DatabaseInner::make_index_name(key, 1, options)?;

        let test_collection_spec = self.internal_get_collection_id_by_name(txn, col_name);
        let mut collection_spec = match test_collection_spec {
            Ok(spec) => spec,
            Err(Error::CollectionNotFound(_)) => {
                let uuid = uuid::Uuid::now_v1(&self.node_id);
                CollectionSpecification::new(col_name.to_string(), uuid)
            }
            Err(err) => {
                return Err(err);
            }
        };

        if collection_spec.indexes.get(&index_name).is_some() {
            return Ok(())
        }

        let index_info = IndexInfo::single_index(
            key.to_string(),
            1,
            options.map(|x| x.clone()),
        );
        collection_spec.indexes.insert(index_name.clone(), index_info.clone());

        DatabaseInner::update_collection_spec(
            col_name,
            &collection_spec,
            txn,
        )?;

        self.build_index(
            txn,
            col_name,
            index_name.as_str(),
            &index_info,
        )
    }

    fn build_index(
        &self,
        txn: &TransactionInner,
        col_name: &str,
        index_name: &str,
        index_info: &IndexInfo,
    ) -> Result<()> {
        let mut builder = IndexBuilder::new(
            txn,
            col_name,
            index_name,
            index_info,
        );

        builder.execute(IndexHelperOperation::Insert)
    }

    pub fn drop_index(&self, col_name: &str, index_name: &str, txn: &TransactionInner) -> Result<()> {
        DatabaseInner::validate_col_name(col_name)?;

        self.internal_drop_index(col_name, index_name, txn)?;

        Ok(())
    }

    fn internal_drop_index(&self, col_name: &str, index_name: &str, txn: &TransactionInner) -> Result<()> {
        let test_collection_spec = self.internal_get_collection_id_by_name(txn, col_name);
        let mut collection_spec = match test_collection_spec {
            Ok(spec) => spec,
            Err(Error::CollectionNotFound(_)) => {
                return Ok(());
            }
            Err(err) => {
                return Err(err);
            }
        };

        let index_info = collection_spec.indexes.get(index_name);
        if index_info.is_none() {
            return Ok(());
        }

        let index_info = index_info.unwrap();

        let mut builder = IndexBuilder::new(
            txn,
            col_name,
            index_name,
            index_info,
        );

        builder.execute(IndexHelperOperation::Delete)?;

        collection_spec.indexes.shift_remove(index_name);

        DatabaseInner::update_collection_spec(
            col_name,
            &collection_spec,
            txn,
        )?;

        Ok(())
    }

    fn update_collection_spec(col_name: &str, collection_spec: &CollectionSpecification, txn: &TransactionInner) -> Result<()> {
        let stacked_key = crate::utils::bson::stacked_key(&[
            Bson::String(TABLE_META_PREFIX.to_string()),
            Bson::String(col_name.to_string()),
        ])?;

        let buffer = bson::to_vec(&collection_spec)?;

        txn.put(stacked_key.as_slice(), buffer.as_ref())?;

        Ok(())
    }

    fn make_index_name(key: &str, order: i32, index_options: Option<&IndexOptions>) -> Result<String> {
        if let Some(options) = index_options {
            if let Some(name) = &options.name {
                DatabaseInner::validate_index_name(name)?;
                return Ok(name.clone());
            }
        }

        let mut index_name = key.to_string().replace(".", "_");

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
        if let Some(id) = doc.get(meta_doc_key::ID) {
            // If the id type is not null, the document is ok
            if id.as_null().is_none() {
                return doc;
            }
        }

        let new_oid = ObjectId::new();
        doc.insert::<String, Bson>(meta_doc_key::ID.into(), new_oid.into());
        doc
    }

    fn validate_col_name(col_name: &str) -> Result<()> {
        for ch in col_name.chars() {
            if ch == '$' || ch == '\n' || ch == '\t' || ch == '\r' || ch == '.' {
                return Err(Error::IllegalCollectionName(col_name.to_string()))
            }
        }

        Ok(())
    }

    fn validate_index_name(col_name: &str) -> Result<()> {
        for ch in col_name.chars() {
            if ch == '$' || ch == '\n' || ch == '\t' || ch == '\r' || ch == '.' {
                return Err(Error::IllegalIndexName(col_name.to_string()))
            }
        }

        Ok(())
    }

    pub fn insert_one(&self, col_name: &str, doc: Document, txn: &TransactionInner) -> Result<InsertOneResult> {
        DatabaseInner::validate_col_name(col_name)?;

        let changed = self.insert_one_internal(txn, col_name, doc, &self.node_id)?;

        Ok(changed)
    }

    fn insert_one_internal(&self, txn: &TransactionInner, col_name: &str, doc: Document, node_id: &[u8; 6]) -> Result<InsertOneResult> {
        let col_meta = self.get_collection_meta_by_name_advanced(txn, col_name, true, node_id)?
            .expect("internal: meta must exist");
        let (result, _) = self.insert_one_with_meta(txn, col_meta, doc)?;
        Ok(result)
    }

    /// Insert one item with the collection spec
    /// return the new spec for the outside to do the following operation
    fn insert_one_with_meta(&self, txn: &TransactionInner, col_spec: CollectionSpecification, doc: Document) -> Result<(InsertOneResult, CollectionSpecification)> {
        let doc  = DatabaseInner::fix_doc(doc);

        let pkey = doc.get("_id").unwrap();

        let stacked_key = crate::utils::bson::stacked_key([
            &Bson::String(col_spec._id.clone()),
            &pkey,
        ])?;

        let doc_buf = bson::to_vec(&doc)?;

        txn.put(
            stacked_key.as_ref(),
            &doc_buf,
        )?;

        self.try_insert_index(txn, &col_spec, &doc, pkey)?;

        Ok((
            InsertOneResult { inserted_id: pkey.clone() },
            col_spec
        ))
    }

    fn try_insert_index(&self, txn: &TransactionInner, col_spec: &CollectionSpecification, doc: &Document, pkey: &Bson) -> Result<()> {
        let mut index_helper = IndexHelper::new(
            txn,
            col_spec,
            doc,
            pkey,
        );
        index_helper.execute(IndexHelperOperation::Insert)
    }

    pub fn insert_many<T: Serialize>(
        &self,
        col_name: &str,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        txn: &TransactionInner,
    ) -> Result<InsertManyResult> {
        DatabaseInner::validate_col_name(col_name)?;

        let result = self.insert_many_internal(txn, col_name, docs, &self.node_id)?;

        Ok(result)
    }

    fn insert_many_internal<T: Serialize>(
        &self,
        txn: &TransactionInner,
        col_name: &str,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        node_id: &[u8; 6],
    ) -> Result<InsertManyResult> {
        let mut col_spec = self.get_collection_meta_by_name_advanced(txn, col_name, true, node_id)?
            .expect("internal: meta must exist");
        let mut inserted_ids: HashMap<usize, Bson> = HashMap::new();
        let mut counter: usize = 0;

        for item in docs {
            let doc = bson::to_document(item.borrow())?;
            let (insert_one_result, new_col_spec) = self.insert_one_with_meta(txn, col_spec, doc)?;
            inserted_ids.insert(counter, insert_one_result.inserted_id);

            counter += 1;
            col_spec = new_col_spec;
        }

        Ok(InsertManyResult {
            inserted_ids,
        })
    }

    fn find_internal<T: DeserializeOwned + Send + Sync>(
        &self,
        col_spec: &CollectionSpecification,
        query: Option<Document>,
        txn: TransactionInner,
    ) -> Result<ClientCursor<T>> {
        let subprogram = match query {
            Some(query) => SubProgram::compile_query(
                col_spec,
                &query,
                true
            ),
            None => SubProgram::compile_query_all(col_spec, true),
        }?;

        let handle = self.make_handle(subprogram, txn)?;
        Ok(handle)
    }

    pub fn update_one(
        &self,
        col_name: &str,
        query: Option<&Document>,
        update: &Document,
        options: UpdateOptions,
        txn: &TransactionInner,
    ) -> Result<UpdateResult> {
        DatabaseInner::validate_col_name(col_name)?;

        let mut txn = txn.clone();
        txn.set_auto_commit(false);
        let result = self.internal_update(col_name, query, update, false, options, &txn)?;

        Ok(result)
    }

    pub(crate) fn update_many(
        &self,
        col_name: &str,
        query: Document,
        update: Document,
        options: UpdateOptions,
        txn: &TransactionInner,
    ) -> Result<UpdateResult> {
        DatabaseInner::validate_col_name(col_name)?;

        let mut txn = txn.clone();
        txn.set_auto_commit(false);
        let result = self.internal_update(
            col_name,
            Some(&query),
            &update,
            true,
            options,
            &txn,
        )?;

        Ok(result)
    }

    fn internal_update(
        &self,
        col_name: &str,
        query: Option<&Document>,
        update: &Document,
        is_many: bool,
        options: UpdateOptions,
        txn: &TransactionInner,
    ) -> Result<UpdateResult> {
        let meta_opt = self.get_collection_meta_by_name_advanced_auto(col_name, false, txn)?;

        let modified_count = match &meta_opt {
            Some(col_spec) => {
                let subprogram = SubProgram::compile_update(
                    col_spec,
                    query,
                    update,
                    true,
                    is_many,
                )?;

                let mut vm = VM::new(
                    txn.clone(),
                    subprogram,
                    self.metrics.clone(),
                );
                vm.execute()?;

                vm.r2 as u64
            },
            None => 0,
        };
        if options.is_upsert() && modified_count == 0 {
            self.upsert(col_name, update, txn)?;
        }

        Ok(UpdateResult {
            modified_count,
        })
    }

    fn upsert(&self, col_name: &str, update: &Document, txn: &TransactionInner) -> Result<()> {
        // extract $set from update
        let set = update.get("$set");
        if set.is_none() {
            return Ok(());
        }

        let set = set.unwrap();

        let doc = set.as_document().ok_or(Error::SetIsNotADocument)?;

        let _insert_result = self.insert_one_internal(txn, col_name, doc.clone(), &self.node_id)?;

        Ok(())
    }
    pub fn drop_collection(&self, col_name: &str, txn: &TransactionInner) -> Result<()> {
        DatabaseInner::validate_col_name(col_name)?;

        self.drop_collection_internal(col_name, txn)?;

        Ok(())
    }

    fn drop_collection_internal(&self, col_name: &str, txn: &TransactionInner) -> Result<()> {
        let test_collection_spec = self.internal_get_collection_id_by_name(txn, col_name);
        let collection_spec = match test_collection_spec {
            Ok(collection_spec) => collection_spec,
            Err(Error::CollectionNotFound(_)) => return Ok(()),
            Err(err) => return Err(err),
        };

        // Delete content begin
        let subprogram = SubProgram::compile_delete_all(
            &collection_spec,
            col_name,
            true,
        )?;

        {
            let mut txn = txn.clone();
            txn.set_auto_commit(false);
            let mut vm = VM::new(
                txn,
                subprogram,
                self.metrics.clone(),
            );
            vm.execute()?;
        } // Delete content end

        self.delete_collection_meta(col_name, txn)?;

        Ok(())
    }

    fn delete_collection_meta(&self, col_name: &str, txn: &TransactionInner) -> Result<()> {
        let mut cursor = {
            let multi_cursor = txn.rocksdb_txn.new_iterator();
            Cursor::new_with_str_prefix(TABLE_META_PREFIX, multi_cursor)?
        };

        let found = cursor.reset_by_pkey(&col_name.into())?;
        if found {
            let key = cursor.peek_key();
            if key.is_none() {
                return Ok(());
            }
            txn.delete(key.unwrap().as_ref())?;
        }

        Ok(())
    }

    pub fn delete(&self, col_name: &str, query: Document, is_many: bool, txn: &TransactionInner) -> Result<usize> {
        DatabaseInner::validate_col_name(col_name)?;
        let mut txn = txn.clone();
        txn.set_auto_commit(false);
        let result = self.internal_delete_by_query(&txn, col_name, query, is_many)?;
        Ok(result)
    }

    fn internal_delete_by_query(&self, txn: &TransactionInner, col_name: &str, query: Document, is_many: bool) -> Result<usize> {
        let col_spec = self.get_collection_meta_by_name_advanced(txn, col_name, true, &self.node_id)?;
        if col_spec.is_none() {
            return Ok(0);
        }
        let col_spec = col_spec.unwrap();

        let subprogram = SubProgram::compile_delete(
            &col_spec,
            col_name,
            Some(&query),
            true,
            is_many,
        )?;

        let mut vm = VM::new(
            txn.clone(),
            subprogram,
            self.metrics.clone(),
        );
        vm.execute()?;

        Ok(vm.r2 as usize)
    }

    fn internal_delete_all(&self, txn: &TransactionInner, col_name: &str) -> Result<usize> {
        let test_collection_spec = self.internal_get_collection_id_by_name(txn, col_name);
        let collection_spec = match test_collection_spec {
            Ok(collection_spec) => collection_spec,
            Err(Error::CollectionNotFound(_)) => return Ok(0),
            Err(err) => return Err(err),
        };

        // Delete content begin
        let subprogram = SubProgram::compile_delete_all(
            &collection_spec,
            col_name,
            true,
        )?;

        let delete_count = {
            let mut vm = VM::new(
                txn.clone(),
                subprogram,
                self.metrics.clone(),
            );
            vm.execute()?;

            vm.r2 as usize
        }; // Delete content end

        Ok(delete_count)
    }

    pub fn delete_all(&self, col_name: &str, txn: &TransactionInner) -> Result<usize> {
        DatabaseInner::validate_col_name(col_name)?;
        let mut txn= txn.clone();
        txn.set_auto_commit(false);
        let result = self.internal_delete_all(&txn, col_name)?;
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

    pub fn count(&self, name: &str, txn: &TransactionInner) -> Result<u64> {
        DatabaseInner::validate_col_name(name)?;

        let col = self.get_collection_meta_by_name_advanced_auto(
            name,
            false,
            txn,
        )?;
        if col.is_none() {
            return Ok(0);
        }

        let col = col.unwrap();
        let mut count = 0;

        let mut handle = self.find_internal::<Document>(&col, None, txn.clone())?;

        while handle.advance()? {
            count += 1;
        }

        Ok(count)
    }

    pub(crate) fn list_collection_names_with_session(&self, txn: &TransactionInner) -> Result<Vec<String>> {
        let docs = self.query_all_meta(txn)?;
        Ok(collection_metas_to_names(docs))
    }

    pub(crate) fn query_all_meta(&self, txn: &TransactionInner) -> Result<Vec<Document>> {
        let mut handle: ClientCursor<Document> = {
            let subprogram = SubProgram::compile_query_all_by_name(
                TABLE_META_PREFIX,
                true
            )?;

            self.make_handle(subprogram, txn.clone())?
        };


        let mut result = Vec::new();

        while handle.advance()? {
            let value = handle.get();
            result.push(value.as_document().unwrap().clone());
        }

        Ok(result)
    }

    pub fn find_with_owned_session<T: DeserializeOwned + Send + Sync>(
        &self,
        col_name: &str,
        filter: impl Into<Option<Document>>,
        txn: TransactionInner,
    ) -> Result<ClientCursor<T>> {
        DatabaseInner::validate_col_name(col_name)?;
        let filter_query = filter.into();
        let meta_opt = self.get_collection_meta_by_name_advanced_auto(
            col_name,
            false,
            &txn,
        )?;
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

        let vm = VM::new(
            txn,
            subprogram,
            self.metrics.clone(),
        );

        let handle = ClientCursor::new(vm);

        Ok(handle)
    }

    pub(crate) fn count_documents(&self, col_name: &str, txn: &TransactionInner) -> Result<u64> {
        DatabaseInner::validate_col_name(col_name)?;
        let test_result = self.count(col_name, txn);
        match test_result {
            Ok(result) => Ok(result),
            Err(Error::CollectionNotFound(_)) => Ok(0),
            Err(err) => Err(err),
        }
    }

    pub(crate) fn delete_one(
        &self,
        col_name: &str,
        query: Document,
        txn: &TransactionInner,
    ) -> Result<DeleteResult> {
        DatabaseInner::validate_col_name(col_name)?;

        let test_count = self.delete(
            col_name,
            query,
            false,
            txn,
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

    pub(crate) fn delete_many(&self, col_name: &str, query: Document, txn: &TransactionInner) -> Result<DeleteResult> {
        DatabaseInner::validate_col_name(col_name)?;

        let test_deleted_count = if query.len() == 0 {
            self.delete_all(col_name, txn)
        } else {
            self.delete(col_name, query, true, txn)
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

    pub(crate) fn aggregate_with_owned_session<T: DeserializeOwned + Send + Sync>(
        &self,
        col_name: &str,
        pipeline: impl IntoIterator<Item = Document>,
        txn: TransactionInner,
    ) -> Result<ClientCursor<T>> {
        DatabaseInner::validate_col_name(col_name)?;
        let meta_opt = self.get_collection_meta_by_name_advanced_auto(col_name, false, &txn)?;
        let subprogram = match meta_opt {
            Some(col_spec) => {
                let subprogram = SubProgram::compile_aggregate(
                    &col_spec,
                    pipeline,
                    true
                )?;

                subprogram
            }
            None => SubProgram::compile_empty_query(),
        };

        let vm = VM::new(
            txn,
            subprogram,
            self.metrics.clone(),
        );

        let handle = ClientCursor::new(vm);

        Ok(handle)
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
    use bson::Bson;

    #[test]
    fn test_validate_col_name() {
        assert!(DatabaseInner::validate_col_name("test").is_ok());
        assert!(DatabaseInner::validate_col_name("$test$").is_err());
        assert!(DatabaseInner::validate_col_name("test\n").is_err());
        assert!(DatabaseInner::validate_col_name("test.ok").is_err());
    }

    #[test]
    fn test_validate_index_name() {
        assert!(DatabaseInner::validate_index_name("test").is_ok());
        assert!(DatabaseInner::validate_index_name("$test$").is_err());
        assert!(DatabaseInner::validate_index_name("test\n").is_err());
        assert!(DatabaseInner::validate_index_name("test.ok").is_err());
    }

    #[test]
    fn test_make_index_name() {
        assert_eq!(DatabaseInner::make_index_name("test", 1, None).unwrap(), "test_1");
        assert_eq!(DatabaseInner::make_index_name("test.ok", 1, None).unwrap(), "test_ok_1");
    }

    #[test]
    fn test_is_is_num_1() {
        assert!(DatabaseInner::is_num_1(&Bson::Int32(1)));
        assert!(!DatabaseInner::is_num_1(&Bson::Int32(2)));
        assert!(!DatabaseInner::is_num_1(&Bson::String("a".to_string())))
    }

}
