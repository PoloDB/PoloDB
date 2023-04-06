/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;
use std::io::Read;
use bson::Bson;
use serde::Serialize;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use crate::error::DbErr;
use crate::{ClientSession, Config};
use super::db_inner::DatabaseInner;
use crate::db::collection::Collection;
use crate::db::db_inner::HandleRequestResult;
use crate::metrics::Metrics;

pub(crate) static SHOULD_LOG: AtomicBool = AtomicBool::new(false);

pub struct IndexedDbContext {
    pub name: String,
    pub idb: web_sys::IdbDatabase,
}

///
/// API wrapper for Rust-level
///
/// Use [`Database::open_file`] API to open a database. A main database file will be
/// generated in the path user provided.
///
/// When you own an instance of a Database, the instance holds a file
/// descriptor of the database file. When the Database instance is dropped,
/// the handle of the file will be released.
///
/// # Collection
/// A [`Collection`] is a dataset of a kind of data.
/// You can use [`Database::create_collection`] to create a data collection.
/// To obtain an exist collection, use [`Database::collection`],
///
pub struct Database {
    inner: Arc<Mutex<DatabaseInner>>,
}

pub type DbResult<T> = Result<T, DbErr>;

impl Database {
    pub fn set_log(v: bool) {
        SHOULD_LOG.store(v, Ordering::SeqCst);
    }

    /// Return the version of package version in string.
    /// Defined in `Cargo.toml`.
    pub fn get_version() -> String {
        const VERSION: &str = env!("CARGO_PKG_VERSION");
        VERSION.into()
    }

    #[cfg(target_arch = "wasm32")]
    pub fn open_indexeddb(ctx: IndexedDbContext) -> DbResult<Database> {
        let inner = DatabaseInner::open_indexeddb(ctx, Config::default())?;

        Ok(Database {
            inner: Mutex::new(inner),
        })
    }

    pub fn open_memory() -> DbResult<Database> {
        Database::open_memory_with_config(Config::default())
    }

    pub fn open_memory_with_config(config: Config) -> DbResult<Database> {
        let inner = DatabaseInner::open_memory(config)?;

        Ok(Database {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_file<P: AsRef<Path>>(path: P) -> DbResult<Database>  {
        Database::open_file_with_config(path, Config::default())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_file_with_config<P: AsRef<Path>>(path: P, config: Config) -> DbResult<Database>  {
        let inner = DatabaseInner::open_file(path.as_ref(), config)?;

        Ok(Database {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Return the metrics object of the database
    pub fn metrics(&self) -> Metrics {
        let inner = self.inner.lock().unwrap();
        inner.metrics()
    }

    /// Creates a new collection in the database with the given `name`.
    pub fn create_collection(&self, name: &str) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        let _ = inner.create_collection(name)?;
        Ok(())
    }

    /// Creates a new collection in the database with the given `name`.
    pub fn create_collection_with_session(&self, name: &str, session: &mut ClientSession) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        let _ = inner.create_collection_internal(name, &mut session.inner)?;
        Ok(())
    }

    ///
    /// [error]: ../enum.DbErr.html
    ///
    /// Return an exist collection. If the collection is not exists,
    /// a new collection will be created.
    ///
    pub fn collection<T: Serialize>(&self, col_name: &str) -> Collection<T> {
        Collection::new(Arc::downgrade(&self.inner), col_name)
    }

    pub fn start_session(&self) -> DbResult<ClientSession> {
        let mut inner = self.inner.lock()?;
        let inner = inner.start_session()?;
        Ok(ClientSession::new(inner))
    }

    /// Gets the names of the collections in the database.
    pub fn list_collection_names(&self) -> DbResult<Vec<String>> {
        let mut inner = self.inner.lock()?;
        let mut session = inner.start_session()?;
        inner.list_collection_names_with_session(&mut session)
    }

    /// Gets the names of the collections in the database.
    pub fn list_collection_names_with_session(&self, session: &mut ClientSession) -> DbResult<Vec<String>> {
        let mut inner = self.inner.lock()?;
        inner.list_collection_names_with_session(&mut session.inner)
    }

    /// handle request for database
    pub fn handle_request<R: Read>(&self, pipe_in: &mut R) -> DbResult<HandleRequestResult> {
        let mut inner = self.inner.lock()?;
        inner.handle_request(pipe_in)
    }

    pub fn handle_request_doc(&self, value: Bson) -> DbResult<HandleRequestResult> {
        let mut inner = self.inner.lock().unwrap();
        inner.handle_request_doc(value)
    }

}

// impl DatabaseInner {
//
//     #[cfg(not(target_arch = "wasm32"))]
//     fn open_file_with_config<P: AsRef<Path>>(path: P, config: Config) -> DbResult<DatabaseInner>  {
//         let ctx = DatabaseInner::open_file(path.as_ref(), config)?;
//
//         Ok(DatabaseInner {
//             ctx,
//         })
//     }
//
//     #[cfg(target_arch = "wasm32")]
//     pub fn open_indexeddb(ctx: IndexedDbContext, config: Config) -> DbResult<DatabaseInner> {
//         let ctx = DatabaseInner::open_indexeddb(ctx, config)?;
//
//         Ok(DatabaseInner {
//             ctx,
//         })
//     }
//
//     fn open_memory_with_config(config: Config) -> DbResult<DatabaseInner> {
//         let  = DatabaseInner::open_memory(config)?;
//
//         Ok(DatabaseInner {
//             ctx,
//         })
//     }
//
//     fn create_collection(&mut self, name: &str, session_id: Option<&ObjectId>) -> DbResult<()> {
//         let _collection_meta = self.in.create_collection_by_id(name, session_id)?;
//         Ok(())
//     }
//
//     #[inline]
//     pub(super) fn get_collection_meta_by_name(
//         &mut self,
//         col_name: &str,
//         create_if_not_exist: bool,
//         session_id: Option<&ObjectId>
//     ) -> DbResult<Option<CollectionSpecification>> {
//         self.ctx.get_collection_meta_by_name_advanced_auto_by_id(col_name, create_if_not_exist, session_id)
//     }
//
//     #[inline]
//     fn start_transaction(&mut self, ty: Option<TransactionType>, session_id: Option<&ObjectId>) -> DbResult<()> {
//         self.ctx.start_transaction(ty, session_id)
//     }
//
//     #[inline]
//     fn commit(&mut self, session_id: Option<&ObjectId>) -> DbResult<()> {
//         self.ctx.commit(session_id)
//     }
//
//     #[inline]
//     fn rollback(&mut self, session_id: Option<&ObjectId>) -> DbResult<()> {
//         self.ctx.rollback(session_id)
//     }
//
//     #[inline]
//     fn drop_session(&mut self, session_id: &ObjectId) -> DbResult<()> {
//         self.ctx.drop_session(session_id)
//     }
//
//     pub(crate) fn query_all_meta(&mut self, session_id: Option<&ObjectId>) -> DbResult<Vec<Document>> {
//         self.ctx.query_all_meta(session_id)
//     }
//
//     fn list_collection_names(&mut self) -> DbResult<Vec<String>> {
//         let doc_meta = self.query_all_meta(None)?;
//         Ok(DatabaseInner::collection_metas_to_names(doc_meta))
//     }
//
//     fn list_collection_names_with_session(&mut self, session: &mut ClientSession) -> DbResult<Vec<String>> {
//         let doc_meta = self.query_all_meta(Some(&session.id))?;
//         Ok(DatabaseInner::collection_metas_to_names(doc_meta))
//     }
//
//     fn collection_metas_to_names(doc_meta: Vec<Document>) -> Vec<String> {
//         doc_meta
//             .iter()
//             .map(|doc| {
//                 let name = doc.get("_id").unwrap().as_str().unwrap().to_string();
//                 name
//             })
//             .collect()
//     }
//
//     fn handle_request<R: Read>(&mut self, pipe_in: &mut R) -> DbResult<HandleRequestResult> {
//         self.handle_request_with_result(pipe_in)
//     }
//
//     fn count_documents(&mut self, name: &str, session_id: Option<&ObjectId>) -> DbResult<u64> {
//         let test_result = self.ctx.count(name, session_id);
//         match test_result {
//             Ok(result) => Ok(result),
//             Err(DbErr::CollectionNotFound(_)) => Ok(0),
//             Err(err) => Err(err),
//         }
//     }
//
//     // fn send_response_with_result<W: Write>(&mut self, pipe_out: &mut W, result: DbResult<HandleRequestResult>, body: Vec<u8>) -> DbResult<HandleRequestResult> {
//     //     match &result {
//     //         Ok(_) => {
//     //             pipe_out.write_u32::<BigEndian>(body.len() as u32)?;
//     //             pipe_out.write(&body)?;
//     //         }
//     //
//     //         Err(err) => {
//     //             pipe_out.write_i32::<BigEndian>(-1)?;
//     //             let str = format!("resp with error: {}", err);
//     //             let str_buf = str.as_bytes();
//     //             pipe_out.write_u32::<BigEndian>(str_buf.len() as u32)?;
//     //             pipe_out.write(str_buf)?;
//     //         }
//     //     }
//     //     result
//     // }
//
//     fn find_one<T: DeserializeOwned>(&mut self, col_name: &str, filter: impl Into<Option<Document>>, session_id: Option<&ObjectId>) -> DbResult<Option<T>> {
//         let filter_query = filter.into();
//         let col_spec = self.get_collection_meta_by_name(col_name, false, session_id)?;
//         let result: Option<T> = if let Some(col_spec) = col_spec {
//             let mut handle = self.ctx.find(
//                 &col_spec,
//                 filter_query,
//                 session_id
//             )?;
//             handle.step()?;
//
//             if !handle.has_row() {
//                 handle.commit_and_close_vm()?;
//                 return Ok(None);
//             }
//
//             let result_doc = handle.get().as_document().unwrap().clone();
//
//             handle.commit_and_close_vm()?;
//
//             bson::from_document(result_doc)?
//         } else {
//             None
//         };
//
//         Ok(result)
//     }
//
//     fn find_many<T: DeserializeOwned>(
//         &mut self, col_name: &str,
//         filter: impl Into<Option<Document>>,
//         session_id: Option<&ObjectId>
//     ) -> DbResult<Vec<T>> {
//         let filter_query = filter.into();
//         let meta_opt = self.get_collection_meta_by_name(col_name, false, session_id)?;
//         match meta_opt {
//             Some(col_spec) => {
//                 let mut handle = self.ctx.find(
//                     &col_spec,
//                     filter_query,
//                     session_id
//                 )?;
//
//                 let mut result: Vec<T> = Vec::new();
//                 consume_handle_to_vec::<T>(&mut handle, &mut result)?;
//
//                 Ok(result)
//
//             }
//             None => {
//                 Ok(vec![])
//             }
//         }
//     }
//
//     fn insert_one<T: Serialize>(&mut self, col_name: &str, doc: impl Borrow<T>, session_id: Option<&ObjectId>) -> DbResult<InsertOneResult> {
//         let doc = bson::to_document(doc.borrow())?;
//         let result = self.ctx.insert_one_auto_by_id(col_name, doc, session_id)?;
//         Ok(result)
//     }
//
//     fn insert_many<T: Serialize>(
//         &mut self,
//         col_name: &str,
//         docs: impl IntoIterator<Item = impl Borrow<T>>,
//         session_id: Option<&ObjectId>
//     ) -> DbResult<InsertManyResult> {
//         self.ctx.insert_many_auto_by_id(col_name, docs, session_id)
//     }
//
//     fn update_one(&mut self, col_name: &str, query: Document, update: Document, session_id: Option<&ObjectId>) -> DbResult<UpdateResult> {
//         let meta_opt = self.get_collection_meta_by_name(col_name, false, session_id)?;
//         let modified_count: u64 = match meta_opt {
//             Some(col_spec) => {
//                 let size = self.ctx.update_one_by_id(
//                     &col_spec,
//                     Some(&query),
//                     &update,
//                     session_id
//                 )?;
//                 size as u64
//             }
//             None => 0,
//         };
//         Ok(UpdateResult {
//             modified_count,
//         })
//     }
//
//     fn update_many(&mut self, col_name: &str, query: Document, update: Document, session_id: Option<&ObjectId>) -> DbResult<UpdateResult> {
//         let meta_opt = self.get_collection_meta_by_name(col_name, false, session_id)?;
//         let modified_count: u64 = match meta_opt {
//             Some(col_spec) => {
//                 let size = self.ctx.update_many_id(
//                     &col_spec,
//                     Some(&query),
//                     &update,
//                     session_id
//                 )?;
//                 size as u64
//             }
//             None => 0,
//         };
//         Ok(UpdateResult {
//             modified_count,
//         })
//     }
//
//     fn delete_one(&mut self, col_name: &str, query: Document, session_id: Option<&ObjectId>) -> DbResult<DeleteResult> {
//         let test_count = self.ctx.delete_by_id(
//             col_name,
//             query,
//             false,
//             session_id,
//         );
//
//         match test_count {
//             Ok(count) => Ok(DeleteResult {
//                 deleted_count: count as u64,
//             }),
//             Err(DbErr::CollectionNotFound(_)) => Ok(DeleteResult {
//                 deleted_count: 0,
//             }),
//             Err(err) => Err(err),
//         }
//
//     }
//
//     fn delete_many(&mut self, col_name: &str, query: Document, session_id: Option<&ObjectId>) -> DbResult<DeleteResult> {
//         let test_deleted_count = if query.len() == 0 {
//             self.ctx.delete_all_by_id(col_name, session_id)
//         } else {
//             self.ctx.delete_by_id(col_name, query, true, session_id)
//         };
//         match test_deleted_count {
//             Ok(deleted_count) => Ok(DeleteResult {
//                 deleted_count: deleted_count as u64,
//             }),
//             Err(DbErr::CollectionNotFound(_)) => Ok(DeleteResult {
//                 deleted_count: 0
//             }),
//             Err(err) => Err(err),
//         }
//     }
//
//     #[inline]
//     fn drop_collection(&mut self, col_name: &str, session_id: Option<&ObjectId>) -> DbResult<()> {
//         self.ctx.drop_collection_by_id(col_name, session_id)
//     }
//
//     /// release in 0.12
//     fn create_index(&mut self, col_name: &str, keys: &Document, options: Option<&Document>, session_id: Option<&ObjectId>) -> DbResult<()> {
//         self.ctx.create_index_id(
//             col_name.into(),
//             keys,
//             options,
//             session_id,
//         )
//     }
//
//     fn receive_request_body<R: Read>(&mut self, pipe_in: &mut R) -> DbResult<Bson> {
//         let request_size = pipe_in.read_u32::<BigEndian>()? as usize;
//         if request_size == 0 {
//             return Ok(Bson::Null);
//         }
//         let mut request_body = vec![0u8; request_size];
//         pipe_in.read_exact(&mut request_body)?;
//         let body_ref: &[u8] = request_body.as_slice();
//         let val = bson::from_slice(body_ref)?;
//         Ok(val)
//     }
//
//     fn handle_start_transaction(&mut self, start_transaction: StartTransactionCommand) -> DbResult<Bson> {
//         self.start_transaction(start_transaction.ty, Some(&start_transaction.session_id))?;
//         Ok(Bson::Null)
//     }
//
//     fn handle_request_with_result<R: Read>(&mut self, pipe_in: &mut R) -> DbResult<HandleRequestResult> {
//         let value = self.receive_request_body(pipe_in)?;
//         self.handle_request_doc(value)
//     }
//
//     fn handle_request_doc(&mut self, value: Bson) -> DbResult<HandleRequestResult> {
//         let command_message = bson::from_bson::<CommandMessage>(value)?;
//
//         let is_quit = if let CommandMessage::SafelyQuit = command_message {
//             true
//         } else {
//             false
//         };
//
//         let result_value: Bson = match command_message {
//             CommandMessage::Find(find) => {
//                 self.handle_find_operation(find)?
//             }
//             CommandMessage::Insert(insert) => {
//                 self.handle_insert_operation(insert)?
//             }
//             CommandMessage::Update(update) => {
//                 self.handle_update_operation(update)?
//             }
//             CommandMessage::Delete(delete) => {
//                 self.handle_delete_operation(delete)?
//             }
//             CommandMessage::CreateCollection(create_collection) => {
//                 self.handle_create_collection(create_collection)?
//             }
//             CommandMessage::DropCollection(drop_collection) => {
//                 self.handle_drop_collection(drop_collection)?
//             }
//             CommandMessage::StartTransaction(start_transaction) => {
//                 self.handle_start_transaction(start_transaction)?
//             }
//             CommandMessage::Commit(commit) => {
//                 self.commit(Some(&commit.session_id))?;
//                 Bson::Null
//             }
//             CommandMessage::Rollback(rollback) => {
//                 self.rollback(Some(&rollback.session_id))?;
//                 Bson::Null
//             }
//             CommandMessage::SafelyQuit => {
//                 Bson::Null
//             }
//             CommandMessage::CountDocuments(count_documents) => {
//                 self.handle_count_operation(count_documents)?
//             }
//         };
//
//         Ok(HandleRequestResult {
//             is_quit,
//             value: result_value,
//         })
//     }
//
//     fn handle_find_operation(&mut self, find: FindCommand) -> DbResult<Bson> {
//         let col_name = find.ns.as_str();
//         let session_id = find.options
//             .as_ref()
//             .map(|o| o.session_id.as_ref())
//             .flatten();
//         let result = if find.multi {
//             self.find_many(col_name, find.filter, session_id)?
//         } else {
//             let result = self.find_one(col_name, find.filter, session_id)?;
//             match result {
//                 Some(doc) => vec![doc],
//                 None => vec![],
//             }
//         };
//
//         let mut value_arr = bson::Array::new();
//
//         for item in result {
//             value_arr.push(Bson::Document(item));
//         }
//
//         let result_value = Bson::Array(value_arr);
//
//         Ok(result_value)
//     }
//
//     fn handle_insert_operation(&mut self, insert: InsertCommand) -> DbResult<Bson> {
//         let col_name = &insert.ns;
//         let session_id = insert.options
//             .as_ref()
//             .map(|o| o.session_id.as_ref())
//             .flatten();
//         let insert_result = self.insert_many(col_name, insert.documents, session_id)?;
//         let bson_val = bson::to_bson(&insert_result)?;
//         Ok(bson_val)
//     }
//
//     fn handle_update_operation(&mut self, update: UpdateCommand) -> DbResult<Bson> {
//         let col_name: &str = &update.ns;
//
//         let session_id = update.options
//             .as_ref()
//             .map(|o| o.session_id.as_ref())
//             .flatten();
//         let result = if update.multi {
//             self.update_many(col_name, update.filter, update.update, session_id)?
//         } else {
//             self.update_one(col_name, update.filter, update.update, session_id)?
//         };
//
//         let bson_val = bson::to_bson(&result)?;
//         Ok(bson_val)
//     }
//
//     fn handle_delete_operation(&mut self, delete: DeleteCommand) -> DbResult<Bson> {
//         let col_name: &str = &delete.ns;
//
//         let session_id = delete.options
//             .as_ref()
//             .map(|o| o.session_id.as_ref())
//             .flatten();
//         let result = if delete.multi {
//             self.delete_many(col_name, delete.filter, session_id)?
//         } else {
//             self.delete_one(col_name, delete.filter, session_id)?
//         };
//
//         let bson_val = bson::to_bson(&result)?;
//         Ok(bson_val)
//     }
//
//     fn handle_create_collection(&mut self, create_collection: CreateCollectionCommand) -> DbResult<Bson> {
//         let ret = match self.create_collection(
//             &create_collection.ns,
//             create_collection.options
//                 .as_ref()
//                 .map(|o| o.session_id.as_ref())
//                 .flatten()
//         ) {
//             Ok(_) => true,
//             Err(DbErr::CollectionAlreadyExits(_)) => false,
//             Err(err) => return Err(err),
//         };
//         Ok(Bson::Boolean(ret))
//     }
//
//     fn handle_drop_collection(&mut self, drop: DropCollectionCommand) -> DbResult<Bson> {
//         let col_name = &drop.ns;
//         let session_id = drop.options
//             .as_ref()
//             .map(|o| o.session_id.as_ref())
//             .flatten();
//         self.ctx.drop_collection_by_id(col_name, session_id)?;
//
//         Ok(Bson::Null)
//     }
//
//     fn handle_count_operation(&mut self, count_documents: CountDocumentsCommand) -> DbResult<Bson> {
//         let count = self.count_documents(
//             &count_documents.ns,
//             count_documents.options
//                 .as_ref()
//                 .map(|o| o.session_id.as_ref())
//                 .flatten()
//         )?;
//         Ok(Bson::Int64(count as i64))
//     }
// }
