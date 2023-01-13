use std::borrow::Borrow;
use std::cell::RefCell;
use std::convert::TryFrom;
use std::path::Path;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;
use bson::{Bson, doc, Document};
use serde::Serialize;
use serde::de::DeserializeOwned;
use byteorder::{self, BigEndian, ReadBytesExt, WriteBytesExt};
use crate::error::DbErr;
use crate::msg_ty::MsgTy;
use crate::Config;
use super::context::{CollectionMeta, DbContext};
use crate::{DbHandle, TransactionType};
use crate::db::collection::Collection;
use crate::dump::FullDump;
use crate::results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};

pub(crate) static SHOULD_LOG: AtomicBool = AtomicBool::new(false);

pub(super) fn consume_handle_to_vec<T: DeserializeOwned>(handle: &mut DbHandle, result: &mut Vec<T>) -> DbResult<()> {
    handle.step()?;

    while handle.has_row() {
        let doc_result = handle.get().as_document().unwrap();
        let item: T = bson::from_document(doc_result.clone())?;
        result.push(item);

        handle.step()?;
    }

    Ok(())
}

macro_rules! unwrap_str_or {
    ($expr: expr, $or: expr) => {
        match $expr {
            Some(Bson::String(id)) => id.as_str(),
            _ => return Err(DbErr::ParseError($or)),
        }
    }
}

///
/// API wrapper for Rust-level
///
/// [open]: #method.open
/// [create_collection]: #method.create_collection
/// [collection]: #method.collection
///
/// Use [open] API to open a database. A main database file will be
/// generated in the path user provided.
///
/// When you own an instance of a Database, the instance holds a file
/// descriptor of the database file. When the Database instance is dropped,
/// the handle of the file will be released.
///
/// # Collection
/// A [Collection](./struct.Collection.html) is a dataset of a kind of data.
/// You can use [create_collection] to create a data collection.
/// To obtain an exist collection, use [collection],
///
/// # Transaction
///
/// [start_transaction]: #method.start_transaction
///
/// You an manually start a transaction by [start_transaction] method.
/// If you don't start it manually, a transaction will be automatically started
/// in your every operation.
///
/// # Example
///
/// ```rust
/// use polodb_core::Database;
/// use polodb_core::bson::{Document, doc};
///
/// # let db_path = polodb_core::test_utils::mk_db_path("doc-test-polo-db");
/// let db = Database::open_file(db_path).unwrap();
/// let collection = db.collection::<Document>("books");
///
/// let docs = vec![
///     doc! { "title": "1984", "author": "George Orwell" },
///     doc! { "title": "Animal Farm", "author": "George Orwell" },
///     doc! { "title": "The Great Gatsby", "author": "F. Scott Fitzgerald" },
/// ];
/// collection.insert_many(docs).unwrap();
/// ```
pub struct Database {
    inner: RefCell<DatabaseInner>,
}

pub(super) struct DatabaseInner {
    pub(super) ctx: DbContext,
}

pub type DbResult<T> = Result<T, DbErr>;

impl Database {
    pub fn set_log(v: bool) {
        SHOULD_LOG.store(v, Ordering::SeqCst);
    }

    pub fn open_memory() -> DbResult<Database> {
        Database::open_memory_with_config(Config::default())
    }

    pub fn open_memory_with_config(config: Config) -> DbResult<Database> {
        let inner = DatabaseInner::open_memory_with_config(config)?;

        Ok(Database {
            inner: RefCell::new(inner),
        })
    }

    pub fn open_file<P: AsRef<Path>>(path: P) -> DbResult<Database>  {
        Database::open_file_with_config(path, Config::default())
    }

    pub fn open_file_with_config<P: AsRef<Path>>(path: P, config: Config) -> DbResult<Database>  {
        let inner = DatabaseInner::open_file_with_config(path, config)?;

        Ok(Database {
            inner: RefCell::new(inner)
        })
    }

    /// Creates a new collection in the database with the given `name`.
    pub fn create_collection<T: Serialize>(&self, name: &str) -> DbResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.create_collection::<T>(name)
    }

    /// Return the version of package version in string.
    /// Defined in `Cargo.toml`.
    pub fn get_version() -> String {
        DbContext::get_version()
    }

    ///
    /// [error]: ../enum.DbErr.html
    ///
    /// Return an exist collection. If the collection is not exists,
    /// a new collection will be created.
    ///
    pub fn collection<T: Serialize>(&self, col_name: &str) -> Collection<T> {
        Collection::new(self, col_name)
    }

    /// Manually start a transaction. There are three types of transaction.
    ///
    /// - `None`: Auto transaction
    /// - `Some(Transaction::Write)`: Write transaction
    /// - `Some(Transaction::Read)`: Read transaction
    ///
    /// When you pass `None` to type parameter. The PoloDB will go into
    /// auto mode. The PoloDB will go into read mode firstly, once the users
    /// execute write operations(insert/update/delete), the DB will turn into
    /// write mode.
    pub fn start_transaction(&self, ty: Option<TransactionType>) -> DbResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.start_transaction(ty)
    }

    pub fn commit(&self) -> DbResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.commit()
    }

    pub fn rollback(&self) -> DbResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.rollback()
    }

    pub fn dump(&self) -> DbResult<FullDump> {
        let mut inner = self.inner.borrow_mut();
        inner.dump()
    }

    pub fn handle_request<R: Read, W: Write>(&self, pipe_in: &mut R, pipe_out: &mut W) -> std::io::Result<MsgTy> {
        let mut inner = self.inner.borrow_mut();
        inner.handle_request(pipe_in, pipe_out)
    }

    #[inline]
    pub(super) fn count_documents(&self, col_name: &str) -> DbResult<u64> {
        let mut inner = self.inner.borrow_mut();
        inner.count_documents(col_name)
    }

    #[inline]
    pub fn find_one<T: DeserializeOwned>(&self, col_name: &str,
                                         filter: impl Into<Option<Document>>) -> DbResult<Option<T>> {
        let mut inner = self.inner.borrow_mut();
        inner.find_one(col_name, filter)
    }

    #[inline]
    pub(super) fn find_many<T: DeserializeOwned>(&self, col_name: &str,
                                                 filter: impl Into<Option<Document>>) -> DbResult<Vec<T>> {
        let mut inner = self.inner.borrow_mut();
        inner.find_many(col_name, filter)
    }

    #[inline]
    pub(super) fn insert_one<T: Serialize>(&self, col_name: &str, doc: impl Borrow<T>) -> DbResult<InsertOneResult> {
        let mut inner = self.inner.borrow_mut();
        inner.insert_one(col_name, doc)
    }

    #[inline]
    pub(super) fn insert_many<T: Serialize>(&self, col_name: &str, docs: impl IntoIterator<Item = impl Borrow<T>>) -> DbResult<InsertManyResult> {
        let mut inner = self.inner.borrow_mut();
        inner.insert_many(col_name, docs)
    }

    #[inline]
    pub(super) fn update_one(&self, col_name: &str, query: Document, update: Document) -> DbResult<UpdateResult> {
        let mut inner = self.inner.borrow_mut();
        inner.update_one(col_name, query, update)
    }

    #[inline]
    pub(super) fn update_many(&self, col_name: &str, query: Document, update: Document) -> DbResult<UpdateResult> {
        let mut inner = self.inner.borrow_mut();
        inner.update_many(col_name, query, update)
    }

    #[inline]
    pub(super) fn delete_one(&self, col_name: &str, query: Document) -> DbResult<DeleteResult> {
        let mut inner = self.inner.borrow_mut();
        inner.delete_one(col_name, query)
    }

    #[inline]
    pub(super) fn delete_many(&self, col_name: &str, query: Document) -> DbResult<DeleteResult> {
        let mut inner = self.inner.borrow_mut();
        inner.delete_many(col_name, query)
    }

    #[inline]
    pub(super) fn create_index(&self, col_name: &str, keys: &Document, options: Option<&Document>) -> DbResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.create_index(col_name, keys, options)
    }
}

impl DatabaseInner {

    fn open_file_with_config<P: AsRef<Path>>(path: P, config: Config) -> DbResult<DatabaseInner>  {
        let ctx = DbContext::open_file(path.as_ref(), config)?;

        Ok(DatabaseInner {
            ctx,
        })
    }

    fn open_memory_with_config(config: Config) -> DbResult<DatabaseInner> {
        let ctx = DbContext::open_memory(config)?;

        Ok(DatabaseInner {
            ctx,
        })
    }

    fn create_collection<T: Serialize>(&mut self, name: &str) -> DbResult<()> {
        let _collection_meta = self.ctx.create_collection(name)?;
        Ok(())
    }

    pub(super) fn get_collection_meta_by_name(&mut self, col_name: &str, create_if_not_exist: bool) -> DbResult<Option<CollectionMeta>> {
        match self.ctx.get_collection_meta_by_name(col_name) {
            Ok(meta) => Ok(Some(meta)),
            Err(DbErr::CollectionNotFound(_)) => {
                if create_if_not_exist {
                    let meta = self.ctx.create_collection(col_name)?;
                    Ok(Some(meta))
                } else {
                    Ok(None)
                }
            },
            Err(err) => return Err(err),
        }
    }

    #[inline]
    pub fn dump(&mut self) -> DbResult<FullDump> {
        self.ctx.dump()
    }

    #[inline]
    fn start_transaction(&mut self, ty: Option<TransactionType>) -> DbResult<()> {
        self.ctx.start_transaction(ty)
    }

    #[inline]
    pub fn commit(&mut self) -> DbResult<()> {
        self.ctx.commit()
    }

    #[inline]
    pub fn rollback(&mut self) -> DbResult<()> {
        self.ctx.rollback()
    }

    #[allow(dead_code)]
    pub(crate) fn query_all_meta(&mut self) -> DbResult<Vec<Document>> {
        self.ctx.query_all_meta()
    }

    /// handle request for database
    /// See [MsgTy] for message detail
    pub fn handle_request<R: Read, W: Write>(&mut self, pipe_in: &mut R, pipe_out: &mut W) -> std::io::Result<MsgTy> {
        let mut buffer: Vec<u8> = Vec::new();
        let result = self.handle_request_with_result(pipe_in, &mut buffer);
        let ret = match &result {
            Ok(t) => t.clone(),
            Err(_) => MsgTy::Undefined,
        };
        if let Err(DbErr::IOErr(io_err)) = result {
            return Err(*io_err);
        }
        let resp_result = self.send_response_with_result(pipe_out, result, buffer);
        if let Err(DbErr::IOErr(io_err)) = resp_result {
            return Err(*io_err);
        }
        if let Err(err) = resp_result {
            eprintln!("resp error: {}", err);
        }
        Ok(ret)
    }

    fn count_documents(&mut self, name: &str) -> DbResult<u64> {
        let meta_opt = self.get_collection_meta_by_name(name, false)?;
        meta_opt.map_or(Ok(0), |col_meta| {
            self.ctx.count(
                col_meta.id,
                col_meta.meta_version
            )
        })
    }

    fn send_response_with_result<W: Write>(&mut self, pipe_out: &mut W, result: DbResult<MsgTy>, body: Vec<u8>) -> DbResult<()> {
        match result {
            Ok(msg_ty) => {
                let val = msg_ty as i32;
                pipe_out.write_i32::<BigEndian>(val)?;
                pipe_out.write_u32::<BigEndian>(body.len() as u32)?;
                pipe_out.write(&body)?;
            }

            Err(err) => {
                pipe_out.write_i32::<BigEndian>(-1)?;
                let str = format!("resp with error: {}", err);
                let str_buf = str.as_bytes();
                pipe_out.write_u32::<BigEndian>(str_buf.len() as u32)?;
                pipe_out.write(str_buf)?;
            }
        }
        Ok(())
    }

    fn find_one<T: DeserializeOwned>(&mut self, col_name: &str, filter: impl Into<Option<Document>>) -> DbResult<Option<T>> {
        let filter_query = filter.into();
        let meta_opt = self.get_collection_meta_by_name(col_name, false)?;
        let result: Option<T> = if let Some(col_meta) = meta_opt {
            let mut handle = self.ctx.find(
                col_meta.id,
                col_meta.meta_version,
                filter_query
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

    fn find_many<T: DeserializeOwned>(&mut self, col_name: &str, filter: impl Into<Option<Document>>) -> DbResult<Vec<T>> {
        let filter_query = filter.into();
        let meta_opt = self.get_collection_meta_by_name(col_name, false)?;
        match meta_opt {
            Some(col_meta) => {
                let mut handle = self.ctx.find(
                    col_meta.id,
                    col_meta.meta_version,
                    filter_query
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

    fn insert_one<T: Serialize>(&mut self, col_name: &str, doc: impl Borrow<T>) -> DbResult<InsertOneResult> {
        let mut doc = bson::to_document(doc.borrow())?;
        let col_meta = self.get_collection_meta_by_name(col_name, true)?
            .expect("internal: meta must exist");
        let _ = self.ctx.insert(col_meta.id, col_meta.meta_version, &mut doc)?;
        let pkey = doc.get("_id").unwrap();
        Ok(InsertOneResult {
            inserted_id: pkey.clone(),
        })
    }

    fn insert_many<T: Serialize>(&mut self, col_name: &str, docs: impl IntoIterator<Item = impl Borrow<T>>) -> DbResult<InsertManyResult> {
        self.start_transaction(Some(TransactionType::Write))?;
        let col_meta = self.get_collection_meta_by_name(col_name, true)?
            .expect("internal: meta must exist");
        let mut inserted_ids: HashMap<usize, Bson> = HashMap::new();
        let mut counter: usize = 0;

        for item in docs {
            let mut doc = match bson::to_document(item.borrow()) {
                Ok(doc) => doc,
                Err(err) => {
                    self.rollback().unwrap();
                    return Err(DbErr::from(err));
                }
            };
            match self.ctx.insert(col_meta.id, col_meta.meta_version, &mut doc) {
                Ok(_) => (),
                Err(err) => {
                    self.rollback().unwrap();
                    return Err(err);
                }
            }
            let pkey = doc.get("_id").unwrap();
            inserted_ids.insert(counter, pkey.clone());

            counter += 1;
        }

        self.commit()?;
        Ok(InsertManyResult {
            inserted_ids,
        })
    }

    fn update_one(&mut self, col_name: &str, query: Document, update: Document) -> DbResult<UpdateResult> {
        let meta_opt = self.get_collection_meta_by_name(col_name, false)?;
        let modified_count: u64 = match meta_opt {
            Some(col_meta) => {
                let size = self.ctx.update_one(
                    col_meta.id,
                    col_meta.meta_version,
                    Some(&query),
                    &update
                )?;
                size as u64
            }
            None => 0,
        };
        Ok(UpdateResult {
            modified_count,
        })
    }

    fn update_many(&mut self, col_name: &str, query: Document, update: Document) -> DbResult<UpdateResult> {
        let meta_opt = self.get_collection_meta_by_name(col_name, false)?;
        let modified_count: u64 = match meta_opt {
            Some(col_meta) => {
                let size = self.ctx.update_many(
                    col_meta.id,
                    col_meta.meta_version,
                    Some(&query),
                    &update
                )?;
                size as u64
            }
            None => 0,
        };
        Ok(UpdateResult {
            modified_count,
        })
    }

    fn delete_one(&mut self, col_name: &str, query: Document) -> DbResult<DeleteResult> {
        let meta_opt = self.get_collection_meta_by_name(col_name, false)?;
        let deleted_count = match meta_opt {
            Some(col_meta) => {
                let count = self.ctx.delete(col_meta.id, col_meta.meta_version,
                                               query, false)?;
                count as u64
            }
            None => 0
        };
        Ok(DeleteResult {
            deleted_count,
        })
    }

    fn delete_many(&mut self, col_name: &str, query: Document) -> DbResult<DeleteResult> {
        let meta_opt = self.get_collection_meta_by_name(col_name, false)?;
        let deleted_count = match meta_opt {
            Some(col_meta) => {
                let count = if query.len() == 0 {
                    self.ctx.delete_all(col_meta.id, col_meta.meta_version)?
                } else {
                    self.ctx.delete(col_meta.id, col_meta.meta_version, query, true)?
                };
                count as u64
            }
            None => 0
        };
        Ok(DeleteResult {
            deleted_count,
        })
    }

    /// release in 0.12
    fn create_index(&mut self, col_name: &str, keys: &Document, options: Option<&Document>) -> DbResult<()> {
        let col_meta = self.get_collection_meta_by_name(col_name, true)?
            .unwrap();
        self.ctx.create_index(col_meta.id, keys, options)
    }

    fn receive_request_body<R: Read>(&mut self, pipe_in: &mut R) -> DbResult<Bson> {
        let request_size = pipe_in.read_u32::<BigEndian>()? as usize;
        if request_size == 0 {
            return Ok(Bson::Null);
        }
        let mut request_body = vec![0u8; request_size];
        pipe_in.read_exact(&mut request_body)?;
        let body_ref: &[u8] = request_body.as_slice();
        let val = bson::from_slice(body_ref)?;
        Ok(val)
    }

    fn handle_start_transaction<R: Read, W: Write>(&mut self, pipe_in: &mut R, _pipe_out: &mut W) -> DbResult<()> {
        let value = self.receive_request_body(pipe_in)?;
        let transaction_type = match value {
            Bson::Int64(val) => val,
            _ => {
                return Err(DbErr::ParseError("transaction needs a type".into()));
            }
        };
        match transaction_type {
            0 => self.start_transaction(None),
            1 => self.start_transaction(Some(TransactionType::Read)),
            2 => self.start_transaction(Some(TransactionType::Write)),
            _ => return Err(DbErr::ParseError("invalid transaction type".into())),
        }
    }

    fn handle_request_with_result<R: Read, W: Write>(&mut self, pipe_in: &mut R, pipe_out: &mut W) -> DbResult<MsgTy> {
        let msg_ty_int = pipe_in.read_i32::<BigEndian>()?;

        let msg_ty = MsgTy::try_from(msg_ty_int)?;

        match msg_ty {
            MsgTy::Find => {
                self.handle_find_operation(pipe_in, pipe_out)?;
            },

            MsgTy::FindOne => {
                self.handle_find_one_operation(pipe_in, pipe_out)?;
            },

            MsgTy::Insert => {
                self.handle_insert_operation(pipe_in, pipe_out)?;
            }

            MsgTy::Update => {
                self.handle_update_operation(pipe_in, pipe_out)?;
            }

            MsgTy::Delete => {
                self.handle_delete_operation(pipe_in, pipe_out)?;
            }

            MsgTy::CreateCollection => {
                self.handle_create_collection(pipe_in, pipe_out)?;
            }

            MsgTy::Drop =>{
                self.handle_drop_collection(pipe_in, pipe_out)?;
            }

            MsgTy::StartTransaction => {
                self.handle_start_transaction(pipe_in, pipe_out)?;
            }

            MsgTy::Commit => {
                self.handle_commit(pipe_in, pipe_out)?;
            }

            MsgTy::Rollback => {
                self.handle_rollback(pipe_in, pipe_out)?;
            }

            MsgTy::Count => {
                self.handle_count_operation(pipe_in, pipe_out)?;
            }

            MsgTy::SafelyQuit => (),

            _ => {
                return Err(DbErr::ParseError("unknown msg type".into()));
            }

        };

        Ok(msg_ty)
    }

    fn handle_find_one_operation<R: Read, W: Write>(&mut self, pipe_in: &mut R, pipe_out: &mut W) -> DbResult<()> {
        let value = self.receive_request_body(pipe_in)?;

        let doc = match value {
            Bson::Document(doc) => doc,
            _ => return Err(DbErr::ParseError(format!("value is not a doc in find one request, actual: {}", value))),
        };

        let collection_name: &str = unwrap_str_or!(doc.get("cl"), "cl not found in find request".into());

        let query_opt = match doc.get("query") {
            Some(Bson::Document(doc)) => doc.clone(),
            _ => return Err(DbErr::ParseError("query not found in find request".into())),
        };

        let result = self.find_one(collection_name, query_opt)?;

        let result_value = match result {
            Some(doc) => Bson::Document(doc),
            None => Bson::Null,
        };

        let bytes = bson::ser::to_vec(&result_value)?;
        pipe_out.write(bytes.as_ref())?;

        Ok(())
    }

    fn handle_find_operation<R: Read, W: Write>(&mut self, pipe_in: &mut R, pipe_out: &mut W) -> DbResult<()> {
        let value = self.receive_request_body(pipe_in)?;

        let doc = match value {
            Bson::Document(doc) => doc,
            _ => return Err(DbErr::ParseError(format!("value is not a doc in find request, actual: {}", value))),
        };

        let collection_name: &str = unwrap_str_or!(doc.get("cl"), "cl not found in find request".into());

        let query_opt = match doc.get("query") {
            Some(Bson::Document(doc)) => Some(doc),
            _ => None,
        };

        let result = if let Some(query) = query_opt {
            self.find_many(collection_name, query.clone())?
        } else {
            self.find_many(collection_name, None)?
        };

        let mut value_arr = bson::Array::new();

        for item in result {
            value_arr.push(Bson::Document(item));
        }

        let result_value = Bson::Array(value_arr);

        let bytes = bson::ser::to_vec(&result_value)?;
        pipe_out.write(bytes.as_ref())?;

        Ok(())
    }

    fn handle_insert_operation<R: Read, W: Write>(&mut self, pipe_in: &mut R, pipe_out: &mut W) -> DbResult<()> {
        let value = self.receive_request_body(pipe_in)?;

        let doc = match value {
            Bson::Document(doc) => doc,
            _ => return Err(DbErr::ParseError(format!("value is not a doc in insert request, actual: {}", value))),
        };

        let collection_name: &str = unwrap_str_or!(doc.get("cl"), "cl not found in find request".into());

        let insert_data = match doc.get("data") {
            Some(Bson::Document(doc)) => doc.clone(),
            _ => return Err(DbErr::ParseError("query not found in insert request".into())),
        };

        let id_changed = self.insert_one(collection_name, insert_data)?;

        let bytes = bson::ser::to_vec(&id_changed.inserted_id)?;
        pipe_out.write(bytes.as_ref())?;

        Ok(())
    }

    fn handle_update_operation<R: Read, W: Write>(&mut self, pipe_in: &mut R, pipe_out: &mut W) -> DbResult<()> {
        let value = self.receive_request_body(pipe_in)?;

        let doc = match value {
            Bson::Document(doc) => doc,
            _ => return Err(DbErr::ParseError(format!("value is not a doc in update request, actual: {}", value))),
        };

        let collection_name: &str = unwrap_str_or!(doc.get("cl"), "cl not found in update request".into());

        let query = match doc.get("query") {
            Some(Bson::Document(doc)) => doc.clone(),
            Some(_) => return Err(DbErr::ParseError("query is not a document in update request".into())),
            None => doc! {}
        };

        let update_data = match doc.get("update") {
            Some(Bson::Document(doc)) => doc.clone(),
            _ => return Err(DbErr::ParseError("'update' not found in update request".into())),
        };

        let result = self.update_many(collection_name, query, update_data)?;

        let ret_val = Bson::Int64(result.modified_count as i64);
        let bytes = bson::ser::to_vec(&ret_val)?;
        pipe_out.write(bytes.as_ref())?;

        Ok(())
    }

    fn handle_delete_operation<R: Read, W: Write>(&mut self, pipe_in: &mut R, pipe_out: &mut W) -> DbResult<()> {
        let value = self.receive_request_body(pipe_in)?;

        let doc = match value {
            Bson::Document(doc) => doc,
            _ => return Err(DbErr::ParseError(format!("value is not a doc in delete request, actual: {}", value))),
        };

        let collection_name: &str = unwrap_str_or!(doc.get("cl"), "cl not found in delete request".into());

        let query = match doc.get("query") {
            Some(Bson::Document(doc)) => doc.clone(),
            Some(_) => return Err(DbErr::ParseError("query is not a document in delete request".into())),
            None => doc! {},
        };

        let result = self.delete_many(collection_name, query)?;

        let ret_val = Bson::Int64(result.deleted_count as i64);
        let bytes = bson::ser::to_vec(&ret_val)?;
        pipe_out.write(bytes.as_ref())?;

        Ok(())
    }

    fn handle_create_collection<R: Read, W: Write>(&mut self, pipe_in: &mut R, pipe_out: &mut W) -> DbResult<()> {
        let value = self.receive_request_body(pipe_in)?;
        let doc: Document = match value {
            Bson::Document(d) => d,
            _ => return Err(DbErr::ParseError(format!("create document expect a document, actual: {}", value))),
        };
        let name: String = match doc.get("name") {
            Some(Bson::String(s)) => s.as_str().into(),
            _ => return Err(DbErr::ParseError(format!("should give the name of the collection to create"))),
        };
        let ret = match self.create_collection::<Bson>(&name) {
            Ok(_) => Bson::Boolean(true),
            Err(DbErr::CollectionAlreadyExits(_)) => Bson::Boolean(false),
            Err(err) => return Err(err),
        };
        let bytes = bson::ser::to_vec(&ret)?;
        pipe_out.write(bytes.as_ref())?;
        Ok(())
    }

    fn handle_drop_collection<R: Read, W: Write>(&mut self, pipe_in: &mut R, _pipe_out: &mut W) -> DbResult<()> {
        let value = self.receive_request_body(pipe_in)?;
        let cl_name: String = match value {
            Bson::String(s) => s.as_str().into(),
            _ => return Err(DbErr::ParseError(format!("should give the name of the collection to drop, actual: {}", value))),
        };
        let info = match self.ctx.get_collection_meta_by_name(&cl_name) {
            Ok(meta) => meta,
            Err(DbErr::CollectionNotFound(_)) => self.ctx.create_collection(&cl_name)?,
            Err(err) => return Err(err),
        };
        self.ctx.drop_collection(info.id, info.meta_version)?;
        Ok(())
    }

    fn handle_count_operation<R: Read, W: Write>(&mut self, pipe_in: &mut R, pipe_out: &mut W) -> DbResult<()> {
        let value = self.receive_request_body(pipe_in)?;

        let doc = match value {
            Bson::Document(doc) => doc,
            _ => return Err(DbErr::ParseError(format!("value is not a doc in count request, actual: {}", value))),
        };

        let collection_name: &str = unwrap_str_or!(doc.get("cl"), "cl not found in count request".into());

        let count = self.count_documents(collection_name)?;

        let ret_val = Bson::Int64(count as i64);
        let bytes = bson::ser::to_vec(&ret_val)?;
        pipe_out.write(bytes.as_ref())?;

        Ok(())
    }

    fn handle_commit<R: Read, W: Write>(&mut self, pipe_in: &mut R, _pipe_out: &mut W) -> DbResult<()> {
        let _ = self.receive_request_body(pipe_in)?;
        self.commit()?;
        Ok(())
    }

    fn handle_rollback<R: Read, W: Write>(&mut self, pipe_in: &mut R, _pipe_out: &mut W) -> DbResult<()> {
        let _ = self.receive_request_body(pipe_in)?;
        self.rollback()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use bson::{Bson, doc, Document};
    use crate::{Config, Database, DbErr};
    use std::io::Read;
    use std::path::PathBuf;
    use std::fs::File;
    use serde::{Deserialize, Serialize};
    use crate::db::collection::Collection;
    use crate::test_utils::{mk_db_path, prepare_db, prepare_db_with_config};

    static TEST_SIZE: usize = 1000;

    fn create_and_return_db_with_items(db_name: &str, size: usize) -> Database {
        let db = prepare_db(db_name).unwrap();
        let collection = db.collection::<Document>("test");

        let mut data: Vec<Document> = vec![];

        for i in 0..size {
            let content = i.to_string();
            let new_doc = doc! {
                "content": content,
            };
            data.push(new_doc);
        }

        collection.insert_many(&data).unwrap();

        db
    }

    #[test]
    fn test_create_collection_and_find_all() {
        let db = create_and_return_db_with_items("test-collection", TEST_SIZE);

        let test_collection = db.collection::<Document>("test");
        let all = test_collection.find_many(None).unwrap();

        let second = test_collection.find_one(doc! {
            "content": "1",
        }).unwrap().unwrap();
        assert_eq!(second.get("content").unwrap().as_str().unwrap(), "1");
        assert!(second.get("content").is_some());

        assert_eq!(TEST_SIZE, all.len())
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Book {
        title: String,
        author: String,
    }

    #[test]
    fn test_insert_struct() {
        let db = prepare_db("test-insert-struct").unwrap();
        // Get a handle to a collection of `Book`.
        let typed_collection = db.collection::<Book>("books");

        let books = vec![
            Book {
                title: "The Grapes of Wrath".to_string(),
                author: "John Steinbeck".to_string(),
            },
            Book {
                title: "To Kill a Mockingbird".to_string(),
                author: "Harper Lee".to_string(),
            },
        ];

        // Insert the books into "mydb.books" collection, no manual conversion to BSON necessary.
        typed_collection.insert_many(books).unwrap();

        let result = typed_collection.find_one(doc! {
            "title": "The Grapes of Wrath",
        }).unwrap();
        let book = result.unwrap();
        assert_eq!(book.author, "John Steinbeck");

        let result = typed_collection.find_many(doc! {
            "$or": [
                {
                    "title": "The Grapes of Wrath",
                },
                {
                    "title": "To Kill a Mockingbird",
                }
            ]
        }).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_transaction_commit() {
        vec![Some("test-transaction-commit"), None].iter().for_each(|value| {
            let db = match value {
                Some(name) => prepare_db(name).unwrap(),
                None => Database::open_memory().unwrap()
            };
            db.start_transaction(None).unwrap();
            let collection = db.collection::<Document>("test");

            for i in 0..10{
                let content = i.to_string();
                let mut new_doc = doc! {
                    "_id": i,
                    "content": content,
                };
                collection.insert_one(&mut new_doc).unwrap();
            }
            db.commit().unwrap()
        });
    }

    #[test]
    fn test_commit_after_commit() {
        let mut config = Config::default();
        config.journal_full_size = 1;
        let db = prepare_db_with_config("test-commit-2", config).unwrap();
        db.start_transaction(None).unwrap();
        let collection = db.collection::<Document>("test");

        for i in 0..1000 {
            let content = i.to_string();
            let new_doc = doc! {
                "_id": i,
                "content": content,
            };
            collection.insert_one(new_doc).unwrap();
        }
        db.commit().unwrap();

        db.start_transaction(None).unwrap();
        let collection2 = db.collection::<Document>("test-2");
        for i in 0..10{
            let content = i.to_string();
            let new_doc = doc! {
                "_id": i,
                "content": content,
            };
            collection2.insert_one(new_doc).expect(&*format!("insert failed: {}", i));
        }
        db.commit().unwrap();
    }

    #[test]
    fn test_multiple_find_one() {
        let db = prepare_db("test-multiple-find-one").unwrap();
        {
            let collection = db.collection("config");
            let doc1 = doc! {
                "_id": "c1",
                "value": "c1",
            };
            collection.insert_one(doc1).unwrap();

            let doc2 = doc! {
                "_id": "c2",
                "value": "c2",
            };
            collection.insert_one(doc2).unwrap();

            let doc2 = doc! {
                "_id": "c3",
                "value": "c3",
            };
            collection.insert_one(doc2).unwrap();

            assert_eq!(collection.count_documents().unwrap(), 3);
        }

        {
            let collection = db.collection::<Document>("config");
            collection.update_many(doc! {
                "_id": "c2"
            }, doc! {
                "$set": doc! {
                    "value": "c33",
                },
            }).unwrap();
            collection.update_many(doc! {
                "_id": "c2",
            }, doc! {
                "$set": doc! {
                    "value": "c22",
                },
            }).unwrap();
        }

        let collection = db.collection::<Document>("config");
        let doc1 = collection.find_one(doc! {
            "_id": "c1",
        }).unwrap().unwrap();

        assert_eq!(doc1.get("value").unwrap().as_str().unwrap(), "c1");

        let collection = db.collection::<Document>("config");
        let doc1 = collection.find_one(doc! {
            "_id": "c2",
        }).unwrap().unwrap();

        assert_eq!(doc1.get("value").unwrap().as_str().unwrap(), "c22");
    }

    #[test]
    fn test_rollback() {
        vec![Some("test-collection"), None].iter().for_each(|value| {
            let db = match value {
                Some(name) => prepare_db(name).unwrap(),
                None => Database::open_memory().unwrap()
            };
            let collection: Collection<Document> = db.collection("test");

            assert_eq!(collection.count_documents().unwrap(), 0);

            db.start_transaction(None).unwrap();

            let collection: Collection<Document> = db.collection("test");
            for i in 0..10 {
                let content = i.to_string();
                let new_doc = doc! {
                    "_id": i,
                    "content": content,
                };
                collection.insert_one(new_doc).unwrap();
            }
            assert_eq!(collection.count_documents().unwrap(), 10);

            db.rollback().unwrap();

            let collection = db.collection::<Document>("test");
            assert_eq!(collection.count_documents().unwrap(), 0);
        });
    }

    #[test]
    fn test_create_collection_with_number_pkey() {
        let db = {
            let db = prepare_db("test-number-pkey").unwrap();
            let collection = db.collection::<Document>("test");
            let mut data: Vec<Document> = vec![];

            for i in 0..TEST_SIZE {
                let content = i.to_string();
                let new_doc = doc! {
                    "_id": i as i64,
                    "content": content,
                };
                data.push(new_doc);
            }

            collection.insert_many(&data).unwrap();

            db
        };

        let collection: Collection<Document> = db.collection::<Document>("test");

        let count = collection.count_documents().unwrap();
        assert_eq!(TEST_SIZE, count as usize);

        let all = collection.find_many(None).unwrap();

        assert_eq!(TEST_SIZE, all.len())
    }

    #[test]
    fn test_find() {
        let db = create_and_return_db_with_items("test-find", TEST_SIZE);
        let collection = db.collection::<Document>("test");

        let result = collection.find_many(
            doc! {
                "content": "3",
            }
        ).unwrap();

        assert_eq!(result.len(), 1);

        let one = result[0].clone();
        assert_eq!(one.get("content").unwrap().as_str().unwrap(), "3");
    }

    #[test]
    fn test_create_collection_and_find_by_pkey() {
        let db = create_and_return_db_with_items("test-find-pkey", 10);
        let collection = db.collection::<Document>("test");

        let all = collection.find_many(None).unwrap();

        assert_eq!(all.len(), 10);

        let first_key = &all[0].get("_id").unwrap();

        let result = collection.find_many(doc! {
            "_id": first_key.clone(),
        }).unwrap();

        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_reopen_db() {
        {
            let _db1 = create_and_return_db_with_items("test-reopen", 5);
        }

        {
            let mut db_path = env::temp_dir();
            db_path.push("test-reopen.db");
            let _db2 = Database::open_file(db_path.as_path().to_str().unwrap()).unwrap();
        }
    }

    #[test]
    fn test_pkey_type_check() {
        let db = create_and_return_db_with_items("test-type-check", TEST_SIZE);

        let doc = doc! {
            "_id": 10,
            "value": "something",
        };

        let collection = db.collection::<Document>("test");
        collection.insert_one(doc).expect_err("should not success");
    }

    #[test]
    fn test_insert_bigger_key() {
        let db = prepare_db("test-insert-bigger-key").unwrap();
        let collection = db.collection("test");

        let mut doc = doc! {};

        let mut new_str: String = String::new();
        for _i in 0..32 {
            new_str.push('0');
        }

        doc.insert::<String, Bson>("_id".into(), Bson::String(new_str.clone()));

        let _ = collection.insert_one(doc).unwrap();
    }

    #[test]
    fn test_db_occupied() {
        const DB_NAME: &'static str = "test-db-lock";
        let db_path = mk_db_path(DB_NAME);
        let _ = std::fs::remove_file(&db_path);
        {
            let config = Config::default();
            let _db1 = Database::open_file_with_config(db_path.as_path().to_str().unwrap(), config).unwrap();
            let config = Config::default();
            let db2 = Database::open_file_with_config(db_path.as_path().to_str().unwrap(), config);
            match db2 {
                Err(DbErr::DatabaseOccupied) => assert!(true),
                Err(other_error) => {
                    println!("{:?}", other_error);
                    assert!(false);
                }
                _ => assert!(false),
            }
        }
        let config = Config::default();
        let _db3 = Database::open_file_with_config(db_path.as_path().to_str().unwrap(), config).unwrap();
    }

    #[test]
    fn test_update_one() {
        let db = prepare_db("test-update-one").unwrap();
        let collection = db.collection::<Document>("test");

        let result = collection.insert_many(vec![
            doc! {
                "name": "Vincent",
                "age": 17,
            },
            doc! {
                "name": "Vincent",
                "age": 18,
            },
        ]).unwrap();

        assert_eq!(result.inserted_ids.len(), 2);

        let result = collection.update_one(doc! {
            "name": "Vincent",
        }, doc! {
            "$set": {
                "name": "Steve",
            }
        }).unwrap();

        assert_eq!(result.modified_count, 1);
    }

    #[test]
    fn test_delete_one() {
        let db = prepare_db("test-update-one").unwrap();
        let collection = db.collection::<Document>("test");

        let result = collection.insert_many(vec![
            doc! {
                "name": "Vincent",
                "age": 17,
            },
            doc! {
                "name": "Vincent",
                "age": 18,
            },
        ]).unwrap();

        assert_eq!(result.inserted_ids.len(), 2);

        let result = collection.delete_one(doc! {
            "name": "Vincent",
        }).unwrap();

        assert_eq!(result.deleted_count, 1);

        let remain = collection.count_documents().unwrap();
        assert_eq!(remain, 1);
    }

    #[test]
    fn test_one_delete_item() {
        let db = prepare_db("test-delete-item").unwrap();
        let collection = db.collection::<Document>("test");

        let mut doc_collection  = vec![];

        for i in 0..100 {
            let content = i.to_string();

            let new_doc = doc! {
                "content": content,
            };

            doc_collection.push(new_doc);
        }
        let result = collection.insert_many(&doc_collection).unwrap();

        let third_key = result.inserted_ids.get(&3).unwrap();
        let delete_doc = doc! {
            "_id": third_key.clone(),
        };
        assert_eq!(collection.delete_many(delete_doc.clone()).unwrap().deleted_count, 1);
        assert_eq!(collection.delete_many(delete_doc).unwrap().deleted_count, 0);
    }

    #[test]
    fn test_delete_all_items() {
        let db = prepare_db("test-delete-all-items").unwrap();
        let collection = db.collection::<Document>("test");

        let mut doc_collection  = vec![];

        for i in 0..1000 {
            let content = i.to_string();
            let new_doc = doc! {
                "_id": i,
                "content": content,
            };
            doc_collection.push(new_doc);
        }
        collection.insert_many(&doc_collection).unwrap();

        for doc in &doc_collection {
            let key = doc.get("_id").unwrap();
            let deleted_result = collection.delete_many(doc!{
                "_id": key.clone(),
            }).unwrap();
            assert!(deleted_result.deleted_count > 0, "delete nothing with key: {}", key);
            let find_doc = doc! {
                "_id": key.clone(),
            };
            let result = collection.find_many(find_doc).unwrap();
            assert_eq!(result.len(), 0, "item with key: {}", key);
        }
    }

    #[test]
    fn test_very_large_binary() {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.pop();
        d.pop();
        d.push("fixtures/test_img.jpg");

        let mut file = File::open(d).unwrap();

        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();

        println!("data size: {}", data.len());
        let db = prepare_db("test-very-large-data").unwrap();
        let collection = db.collection("test");

        let mut doc = doc! {};
        let origin_data = data.clone();
        doc.insert::<String, Bson>("content".into(), Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: origin_data.clone(),
        }));

        let result = collection.insert_one(doc).unwrap();

        let new_id = result.inserted_id;
        let back = collection.find_one(doc! {
            "_id": new_id,
        }).unwrap().unwrap();

        let back_bin = back.get("content").unwrap();

        let binary = match back_bin {
            bson::Bson::Binary(bin) => {
                bin
            }
            _ => panic!("type unmatched"),
        };
        assert_eq!(&binary.bytes, &origin_data);
    }

}
