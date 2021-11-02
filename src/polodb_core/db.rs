use std::convert::TryFrom;
use std::rc::Rc;
use std::path::Path;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use polodb_bson::{Document, ObjectId, Value};
use byteorder::{self, BigEndian, ReadBytesExt, WriteBytesExt};
use super::error::DbErr;
use crate::msg_ty::MsgTy;
use crate::Config;
use crate::context::DbContext;
use crate::{DbHandle, TransactionType};
use crate::dump::FullDump;

pub(crate) static SHOULD_LOG: AtomicBool = AtomicBool::new(false);

fn consume_handle_to_vec(handle: &mut DbHandle, result: &mut Vec<Rc<Document>>) -> DbResult<()> {
    handle.step()?;

    while handle.has_row() {
        let doc = handle.get().unwrap_document();
        result.push(doc.clone());

        handle.step()?;
    }

    Ok(())
}

macro_rules! unwrap_str_or {
    ($expr: expr, $or: expr) => {
        match $expr {
            Some(Value::String(id)) => id.as_str(),
            _ => return Err(DbErr::ParseError($or)),
        }
    }
}

/// A wrapper of collection in struct.
///
/// All CURD methods can be done through this structure.
///
/// Find/Update/Delete operations need a query object.
///
/// ## Query operation:
///
/// | Name | Description |
/// | ----------| ----------- |
/// | $eq | Matches values that are equal to a specified value. |
/// | $gt | Matches values that are greater than a specified value. |
/// | $gte | Matches values that are greater than or equal to a specified value. |
/// | $in | Matches any of the values specified in an array. |
/// | $lt | Matches values that are less than a specified value. |
/// | $lte | Matches values that are less than or equal to a specified value. |
/// | $ne | Matches all values that are not equal to a specified value. |
/// | $nin | Matches none of the values specified in an array. |
///
/// ## Logical operation:
///
/// | Name | Description |
/// | ---- | ----------- |
/// | $and | Joins query clauses with a logical AND returns all documents that match the conditions of both clauses. |
/// | $or | Joins query clauses with a logical OR returns all documents that match the conditions of either clause. |
///
/// ## Example:
///
/// ```rust
/// use std::rc::Rc;
/// use polodb_core::Database;
/// use polodb_bson::doc;
///
/// let mut db = Database::open_file("/tmp/test-collection").unwrap();
/// let mut collection = db.collection("test").unwrap();
/// collection.insert(doc! {
///     "_id": 0,
///     "name": "Vincent Chan",
///     "score": 99.99,
/// }.as_mut());
/// ```
pub struct Collection<'a> {
    db: &'a mut Database,
    id: u32,
    meta_version: u32,
    name: String,
}

impl<'a>  Collection<'a> {

    fn new(db: &'a mut Database, id: u32, meta_version: u32, name: &str) -> Collection<'a> {
        Collection {
            db,
            id,
            meta_version,
            name: name.into(),
        }
    }

    /// all the data in the collection return.
    pub fn find_all(&mut self) -> DbResult<Vec<Rc<Document>>> {
        let mut handle = self.db.ctx.find(self.id, self.meta_version, None)?;

        let mut result = Vec::new();

        consume_handle_to_vec(&mut handle, &mut result)?;

        Ok(result)
    }

    /// When query document is passed to the function. The result satisfies
    /// the query document.
    pub fn find(&mut self, query: &Document) -> DbResult<Vec<Rc<Document>>> {
        let mut handle = self.db.ctx.find(
            self.id, self.meta_version, Some(query)
        )?;

        let mut result = Vec::new();

        consume_handle_to_vec(&mut handle, &mut result)?;

        Ok(result)
    }

    /// Return the first element in the collection satisfies the query.
    pub fn find_one(&mut self, query: &Document) -> DbResult<Option<Rc<Document>>> {
        let mut handle = self.db.ctx.find(
              self.id, self.meta_version, Some(query)
        )?;
        handle.step()?;

        if !handle.has_row() {
            handle.commit_and_close_vm()?;
            return Ok(None);
        }

        let result = handle.get().unwrap_document().clone();

        handle.commit_and_close_vm()?;

        Ok(Some(result))
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the size of all data in the collection.
    #[inline]
    pub fn count(&mut self) -> DbResult<u64> {
        self.db.ctx.count(self.id, self.meta_version)
    }

    /// When query is `None`, all the data in the collection will be updated.
    /// Basically the same as [MongoDB](https://docs.mongodb.com/manual/reference/operator/update-field/).
    ///
    /// ## Field Update Operators:
    ///
    /// | Name | Description |
    /// | ---- | ----------- |
    /// | $inc | Increments the value of the field by the specified amount. |
    /// | $min | Only updates the field if the specified value is less than the existing field value. |
    /// | $max | Only updates the field if the specified value is greater than the existing field value. |
    /// | $mul | Multiplies the value of the field by the specified amount. |
    /// | $rename | Renames a field. |
    /// | $set | Sets the value of a field in a document. |
    /// | $unset | Removes the specified field from a document. |
    #[inline]
    pub fn update(&mut self, query: Option<&Document>, update: &Document) -> DbResult<usize> {
        self.db.ctx.update(self.id, self.meta_version, query, update)
    }

    /// Insert a document into the database.
    /// The returning boolean value represents if the DB inserted a "_id" for you.
    #[inline]
    pub fn insert(&mut self, doc: &mut Document) -> DbResult<bool> {
        self.db.ctx.insert(self.id, self.meta_version, doc)
    }

    /// When query is `None`, all the data in the collection will be deleted.
    ///
    /// The size of data deleted returns.
    #[inline]
    pub fn delete(&mut self, query: Option<&Document>) -> DbResult<usize> {
        match query {
            Some(query) =>
                self.db.ctx.delete(self.id, self.meta_version, query),
            None =>
                self.db.ctx.delete_all(self.id, self.meta_version),
        }
    }

    /// release in 0.12
    #[allow(dead_code)]
    fn create_index(&mut self, keys: &Document, options: Option<&Document>) -> DbResult<()> {
        self.db.ctx.create_index(self.id, keys, options)
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
///
/// let mut db = Database::open_file("/tmp/test-polo.db").unwrap();
/// let test_collection = db.collection("test").unwrap();
/// ```
pub struct Database {
    ctx: Box<DbContext>,
}

pub type DbResult<T> = Result<T, DbErr>;

impl Database {

    #[inline]
    pub fn set_log(v: bool) {
        SHOULD_LOG.store(v, Ordering::SeqCst);
        eprintln!("set log");
    }

    #[inline]
    pub fn mk_object_id(&mut self) -> ObjectId {
        self.ctx.object_id_maker().mk_object_id()
    }

    #[deprecated]
    pub fn open<P: AsRef<Path>>(path: P) -> DbResult<Database>  {
        Database::open_file(path)
    }

    #[deprecated]
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: Config) -> DbResult<Database>  {
        Database::open_file_with_config(path, config)
    }

    pub fn open_file<P: AsRef<Path>>(path: P) -> DbResult<Database>  {
        Database::open_file_with_config(path, Config::default())
    }

    pub fn open_file_with_config<P: AsRef<Path>>(path: P, config: Config) -> DbResult<Database>  {
        let ctx = DbContext::open_file(path.as_ref(), config)?;
        let rc_ctx = Box::new(ctx);

        Ok(Database {
            ctx: rc_ctx,
        })
    }

    pub fn open_memory() -> DbResult<Database> {
        Database::open_memory_wht_config(Config::default())
    }

    pub fn open_memory_wht_config(config: Config) -> DbResult<Database> {
        let ctx = DbContext::open_memory(config)?;
        let rc_ctx = Box::new(ctx);

        Ok(Database {
            ctx: rc_ctx,
        })
    }

    pub fn create_collection(&mut self, name: &str) -> DbResult<Collection> {
        let collection_meta = self.ctx.create_collection(name)?;
        Ok(Collection::new(self,
                           collection_meta.id,
                           collection_meta.meta_version,
                           name))
    }

    /// Return the version of package version in string.
    /// Defined in `Cargo.toml`.
    #[inline]
    pub fn get_version() -> String {
        DbContext::get_version()
    }

    ///
    /// [error]: ../enum.DbErr.html
    ///
    /// Return an exist collection. If the collection is not exists,
    /// a new collection will be created.
    ///
    pub fn collection(&mut self, col_name: &str) -> DbResult<Collection> {
        let info = match self.ctx.get_collection_meta_by_name(col_name) {
            Ok(meta) => meta,
            Err(DbErr::CollectionNotFound(_)) => self.ctx.create_collection(col_name)?,
            Err(err) => return Err(err),
        };
        Ok(Collection::new(self, info.id, info.meta_version, col_name))
    }

    #[inline]
    pub fn dump(&mut self) -> DbResult<FullDump> {
        self.ctx.dump()
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
    #[inline]
    pub fn start_transaction(&mut self, ty: Option<TransactionType>) -> DbResult<()> {
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
    pub(crate) fn query_all_meta(&mut self) -> DbResult<Vec<Rc<Document>>> {
        self.ctx.query_all_meta()
    }

    /// Upgrade DB from v1 to v2
    /// The older file will be renamed as (name).old
    pub fn v1_to_v2(path: &Path) -> DbResult<()> {
        crate::migration::v1_to_v2(path)
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

    fn handle_start_transaction<R: Read, W: Write>(&mut self, pipe_in: &mut R, pipe_out: &mut W) -> DbResult<()> {
        let value = self.receive_request_body(pipe_in)?;
        let transaction_type = match value {
            Value::Int(val) => val,
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

    fn handle_commit<R: Read, W: Write>(&mut self, _pipe_in: &mut R, _pipe_out: &mut W) -> DbResult<()> {
        self.commit()?;
        Ok(())
    }

    fn handle_rollback<R: Read, W: Write>(&mut self, _pipe_in: &mut R, _pipe_out: &mut W) -> DbResult<()> {
        self.rollback()?;
        Ok(())
    }

    fn handle_find_one_operation<R: Read, W: Write>(&mut self, pipe_in: &mut R, pipe_out: &mut W) -> DbResult<()> {
        let value = self.receive_request_body(pipe_in)?;

        let doc = match value {
            Value::Document(doc) => doc,
            _ => return Err(DbErr::ParseError(format!("value is not a doc in find one request, actual: {}", value))),
        };

        let collection_name: &str = unwrap_str_or!(doc.get("cl"), "cl not found in find request".into());

        let mut query_opt = match doc.get("query") {
            Some(Value::Document(doc)) => doc.clone(),
            _ => return Err(DbErr::ParseError("query not found in find request".into())),
        };

        let mut_doc = Rc::make_mut(&mut query_opt);
        let mut collection = self.collection(collection_name)?;

        let result = collection.find_one(mut_doc)?;

        let result_value = match result {
            Some(doc) => Value::Document(doc),
            None => Value::Null,
        };

        result_value.to_msgpack(pipe_out)?;

        Ok(())
    }

    fn receive_request_body<R: Read>(&mut self, pipe_in: &mut R) -> DbResult<Value> {
        let request_size = pipe_in.read_u32::<BigEndian>()? as usize;
        if request_size == 0 {
            return Ok(Value::Null);
        }
        let mut request_body = vec![0u8; request_size];
        pipe_in.read_exact(&mut request_body)?;
        let mut body_ref: &[u8] = request_body.as_slice();
        let val = Value::from_msgpack(&mut body_ref)?;
        Ok(val)
    }

    fn handle_find_operation<R: Read, W: Write>(&mut self, pipe_in: &mut R, pipe_out: &mut W) -> DbResult<()> {
        let value = self.receive_request_body(pipe_in)?;

        let doc = match value {
            Value::Document(doc) => doc,
            _ => return Err(DbErr::ParseError(format!("value is not a doc in find request, actual: {}", value))),
        };

        let collection_name: &str = unwrap_str_or!(doc.get("cl"), "cl not found in find request".into());

        let query_opt = match doc.get("query") {
            Some(Value::Document(doc)) => Some(doc),
            _ => None,
        };

        let mut collection = self.collection(collection_name)?;

        let result = if let Some(query) = query_opt {
            collection.find(query)?
        } else {
            collection.find_all()?
        };

        let mut value_arr = polodb_bson::Array::new();

        for item in result {
            value_arr.push(Value::Document(item));
        }

        let result_value = Value::Array(Rc::new(value_arr));
        result_value.to_msgpack(pipe_out)?;

        Ok(())
    }

    fn handle_insert_operation<R: Read, W: Write>(&mut self, pipe_in: &mut R, pipe_out: &mut W) -> DbResult<()> {
        let value = self.receive_request_body(pipe_in)?;

        let doc = match value {
            Value::Document(doc) => doc,
            _ => return Err(DbErr::ParseError(format!("value is not a doc in insert request, actual: {}", value))),
        };

        let collection_name: &str = unwrap_str_or!(doc.get("cl"), "cl not found in find request".into());

        let mut insert_data = match doc.get("data") {
            Some(Value::Document(doc)) => doc.clone(),
            _ => return Err(DbErr::ParseError("query not found in insert request".into())),
        };

        let mut_doc = Rc::make_mut(&mut insert_data);

        let mut collection = self.collection(collection_name)?;
        let id_changed = collection.insert(mut_doc)?;

        let ret_value = if id_changed {
            mut_doc.get("_id").unwrap().clone()
        } else {
            Value::Null
        };

        ret_value.to_msgpack(pipe_out)?;

        Ok(())
    }

    fn handle_update_operation<R: Read, W: Write>(&mut self, pipe_in: &mut R, pipe_out: &mut W) -> DbResult<()> {
        let value = self.receive_request_body(pipe_in)?;

        let doc = match value {
            Value::Document(doc) => doc,
            _ => return Err(DbErr::ParseError(format!("value is not a doc in update request, actual: {}", value))),
        };

        let collection_name: &str = unwrap_str_or!(doc.get("cl"), "cl not found in update request".into());

        let query = match doc.get("query") {
            Some(Value::Document(doc)) => Some(doc.as_ref()),
            Some(_) => return Err(DbErr::ParseError("query is not a document in update request".into())),
            None => None
        };

        let update_data = match doc.get("update") {
            Some(Value::Document(doc)) => doc.clone(),
            _ => return Err(DbErr::ParseError("'update' not found in update request".into())),
        };

        let mut collection = self.collection(collection_name)?;
        let size = collection.update(query, update_data.as_ref())?;

        let ret_val = Value::Int(size as i64);
        ret_val.to_msgpack(pipe_out)?;

        Ok(())
    }

    fn handle_delete_operation<R: Read, W: Write>(&mut self, pipe_in: &mut R, pipe_out: &mut W) -> DbResult<()> {
        let value = self.receive_request_body(pipe_in)?;

        let doc = match value {
            Value::Document(doc) => doc,
            _ => return Err(DbErr::ParseError(format!("value is not a doc in delete request, actual: {}", value))),
        };

        let collection_name: &str = unwrap_str_or!(doc.get("cl"), "cl not found in delete request".into());

        let query = match doc.get("query") {
            Some(Value::Document(doc)) => Some(doc.as_ref()),
            Some(_) => return Err(DbErr::ParseError("query is not a document in delete request".into())),
            None => None
        };

        let mut collection = self.collection(collection_name)?;
        let size = collection.delete(query)?;

        let ret_val = Value::Int(size as i64);
        ret_val.to_msgpack(pipe_out)?;

        Ok(())
    }

    fn handle_count_operation<R: Read, W: Write>(&mut self, pipe_in: &mut R, pipe_out: &mut W) -> DbResult<()> {
        let value = self.receive_request_body(pipe_in)?;

        let doc = match value {
            Value::Document(doc) => doc,
            _ => return Err(DbErr::ParseError(format!("value is not a doc in count request, actual: {}", value))),
        };

        let collection_name: &str = unwrap_str_or!(doc.get("cl"), "cl not found in count request".into());

        let mut collection = self.collection(collection_name)?;

        let count = collection.count()?;

        let ret_val = Value::Int(count as i64);
        ret_val.to_msgpack(pipe_out)?;

        Ok(())
    }

}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::env;
    use polodb_bson::{Document, Value, doc};
    use crate::{Database, Config, DbResult, DbErr};
    use std::io::Read;
    use std::path::PathBuf;
    use std::fs::File;

    static TEST_SIZE: usize = 1000;

    fn mk_db_path(db_name: &str) -> PathBuf {
        let mut db_path = env::temp_dir();
        let db_filename = String::from(db_name) + ".db";
        db_path.push(db_filename);
        db_path
    }

    fn mk_journal_path(db_name: &str) -> PathBuf {
        let mut journal_path = env::temp_dir();

        let journal_filename = String::from(db_name) + ".db.journal";
        journal_path.push(journal_filename);

        journal_path
    }

    fn prepare_db_with_config(db_name: &str, config: Config) -> DbResult<Database> {
        let db_path = mk_db_path(db_name);
        let journal_path = mk_journal_path(db_name);

        let _ = std::fs::remove_file(db_path.as_path());
        let _ = std::fs::remove_file(journal_path);

        Database::open_file_with_config(db_path.as_path().to_str().unwrap(), config)
    }

    fn prepare_db(db_name: &str) -> DbResult<Database> {
        prepare_db_with_config(db_name, Config::default())
    }

    fn create_and_return_db_with_items(db_name: &str, size: usize) -> Database {
        let mut db = prepare_db(db_name).unwrap();
        let mut collection = db.create_collection("test").unwrap();

        // let meta = db.query_all_meta().unwrap();

        for i in 0..size {
            let content = i.to_string();
            let mut new_doc = doc! {
                "content": content,
            };
            collection.insert(&mut new_doc).unwrap();
        }

        db
    }

    #[test]
    fn test_create_collection_and_find_all() {
        let mut db = create_and_return_db_with_items("test-collection", TEST_SIZE);

        let mut test_collection = db.collection("test").unwrap();
        let all = test_collection.find_all( ).unwrap();

        let second = test_collection.find_one(&doc! {
            "content": "1",
        }).unwrap().unwrap();
        assert_eq!(second.get("content").unwrap().unwrap_string(), "1");
        assert!(second.get("content").is_some());

        assert_eq!(TEST_SIZE, all.len())
    }

    #[test]
    fn test_transaction_commit() {
        vec![Some("test-transaction-commit"), None].iter().for_each(|value| {
            let mut db = match value {
                Some(name) => prepare_db(name).unwrap(),
                None => Database::open_memory().unwrap()
            };
            db.start_transaction(None).unwrap();
            let mut collection = db.create_collection("test").unwrap();

            for i in 0..10{
                let content = i.to_string();
                let mut new_doc = doc! {
                    "_id": i,
                    "content": content,
                };
                collection.insert(&mut new_doc).unwrap();
            }
            db.commit().unwrap()
        });
    }

    #[test]
    fn test_commit_after_commit() {
        let mut config = Config::default();
        config.journal_full_size = 1;
        let mut db = prepare_db_with_config("test-commit-2", config).unwrap();
        db.start_transaction(None).unwrap();
        let mut collection = db.create_collection("test").unwrap();

        for i in 0..1000 {
            let content = i.to_string();
            let mut new_doc = doc! {
                "_id": i,
                "content": content,
            };
            collection.insert(&mut new_doc).unwrap();
        }
        db.commit().unwrap();

        db.start_transaction(None).unwrap();
        let mut collection2 = db.create_collection("test-2").unwrap();
        for i in 0..10{
            let content = i.to_string();
            let mut new_doc = doc! {
                "_id": i,
                "content": content,
            };
            collection2.insert(&mut new_doc).expect(&*format!("insert failed: {}", i));
        }
        db.commit().unwrap();
    }

    #[test]
    fn test_multiple_find_one() {
        let mut db = prepare_db("test-multiple-find-one").unwrap();
        {
            let mut collection = db.collection("config").unwrap();
            let mut doc1 = doc! {
                "_id": "c1",
                "value": "c1",
            };
            collection.insert(&mut doc1).unwrap();

            let mut doc2 = doc! {
                "_id": "c2",
                "value": "c2",
            };
            collection.insert(&mut doc2).unwrap();

            let mut doc2 = doc! {
                "_id": "c3",
                "value": "c3",
            };
            collection.insert(&mut doc2).unwrap();

            assert_eq!(collection.count().unwrap(), 3);
        }

        {
            let mut collection = db.collection("config").unwrap();
            collection.update(Some(&doc! {
                "_id": "c2",
            }), &doc! {
                "$set": doc! {
                    "value": "c33",
                },
            }).unwrap();
            collection.update(Some(&doc! {
                "_id": "c2",
            }), &doc! {
                "$set": doc! {
                    "value": "c22",
                },
            }).unwrap();
        }

        let mut collection = db.collection("config").unwrap();
        let doc1 = collection.find_one(&doc! {
            "_id": "c1",
        }).unwrap().unwrap();

        assert_eq!(doc1.get("value").unwrap().unwrap_string(), "c1");

        let mut collection = db.collection("config").unwrap();
        let doc1 = collection.find_one(&doc! {
            "_id": "c2",
        }).unwrap().unwrap();

        assert_eq!(doc1.get("value").unwrap().unwrap_string(), "c22");
    }

    #[test]
    fn test_rollback() {
        vec![Some("test-collection"), None].iter().for_each(|value| {
            let mut db = match value {
                Some(name) => prepare_db(name).unwrap(),
                None => Database::open_memory().unwrap()
            };
            let mut collection = db.create_collection("test").unwrap();

            assert_eq!(collection.count().unwrap(), 0);

            db.start_transaction(None).unwrap();

            let mut collection = db.collection("test").unwrap();
            for i in 0..10 {
                let content = i.to_string();
                let mut new_doc = doc! {
                "_id": i,
                "content": content,
            };
                collection.insert(new_doc.as_mut()).unwrap();
            }
            assert_eq!(collection.count().unwrap(), 10);

            db.rollback().unwrap();

            let mut collection = db.collection("test").unwrap();
            assert_eq!(collection.count().unwrap(), 0);
        });
    }

    #[test]
    fn test_create_collection_with_number_pkey() {
        let mut db = {
            let mut db = prepare_db("test-number-pkey").unwrap();
            let mut collection = db.create_collection("test").unwrap();

            for i in 0..TEST_SIZE {
                let content = i.to_string();
                let mut new_doc = doc! {
                    "_id": i,
                    "content": content,
                };
                collection.insert(new_doc.as_mut()).unwrap();
            }

            db
        };

        let mut collection = db.collection("test").unwrap();

        let count = collection.count().unwrap();
        assert_eq!(TEST_SIZE, count as usize);

        let all = collection.find_all( ).unwrap();

        assert_eq!(TEST_SIZE, all.len())
    }

    #[test]
    fn test_find() {
        let mut db = create_and_return_db_with_items("test-find", TEST_SIZE);
        let mut collection = db.collection("test").unwrap();

        let result = collection.find(
            &doc! {
                "content": "3",
            }
        ).unwrap();

        assert_eq!(result.len(), 1);

        let one = result[0].clone();
        assert_eq!(one.get("content").unwrap().unwrap_string(), "3");
    }

    #[test]
    fn test_create_collection_and_find_by_pkey() {
        let mut db = create_and_return_db_with_items("test-find-pkey", 10);
        let mut collection = db.collection("test").unwrap();

        let all = collection.find_all().unwrap();

        assert_eq!(all.len(), 10);

        let first_key = &all[0].pkey_id().unwrap();

        let result = collection.find(&doc! {
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
        let mut db = create_and_return_db_with_items("test-type-check", TEST_SIZE);

        let mut doc = doc! {
            "_id": 10,
            "value": "something",
        };

        let mut collection = db.collection("test").unwrap();
        collection.insert(doc.as_mut()).expect_err("should not success");
    }

    #[test]
    fn test_insert_bigger_key() {
        let mut db = prepare_db("test-insert-bigger-key").unwrap();
        let mut collection = db.create_collection("test").unwrap();

        let mut doc = Document::new_without_id();

        let mut new_str: String = String::new();
        for _i in 0..32 {
            new_str.push('0');
        }

        doc.insert("_id".into(), Value::String(Rc::new(new_str.clone())));

        let _ = collection.insert(doc.as_mut()).unwrap();
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
                _ => assert!(false),
            }
        }
        let config = Config::default();
        let _db3 = Database::open_file_with_config(db_path.as_path().to_str().unwrap(), config).unwrap();
    }

    #[test]
    fn test_create_index() {
        let mut db = prepare_db("test-create-index").unwrap();
        let mut collection = db.create_collection("test").unwrap();

        let keys = doc! {
            "user_id": 1,
        };

        collection.create_index(&keys, None).unwrap();

        for i in 0..10 {
            let str = Rc::new(i.to_string());
            let mut data = doc! {
                "name": str.clone(),
                "user_id": str.clone(),
            };
            collection.insert(data.as_mut()).unwrap();
        }

        let mut data = doc! {
            "name": "what",
            "user_id": 3,
        };
        collection.insert(data.as_mut()).expect_err("not comparable");
    }

    #[test]
    fn test_one_delete_item() {
        let mut db = prepare_db("test-delete-item").unwrap();
        let mut collection = db.create_collection("test").unwrap();

        let mut doc_collection  = vec![];

        for i in 0..100 {
            let content = i.to_string();

            let mut new_doc = doc! {
                "content": content,
            };

            collection.insert(new_doc.as_mut()).unwrap();
            doc_collection.push(new_doc);
        }

        let third = &doc_collection[3];
        let third_key = third.get("_id").unwrap();
        let delete_doc = doc! {
            "_id": third_key.clone(),
        };
        assert!(collection.delete(Some(&delete_doc)).unwrap() > 0);
        assert_eq!(collection.delete(Some(&delete_doc)).unwrap(), 0);
    }

    #[test]
    fn test_delete_all_items() {
        let mut db = prepare_db("test-delete-all-items").unwrap();
        let mut collection = db.create_collection("test").unwrap();

        let mut doc_collection  = vec![];

        for i in 0..1000 {
            let content = i.to_string();
            let mut new_doc = doc! {
                "_id": i,
                "content": content,
            };
            collection.insert(new_doc.as_mut()).unwrap();
            doc_collection.push(new_doc);
        }

        for doc in &doc_collection {
            let key = doc.get("_id").unwrap();
            let deleted = collection.delete(Some(&doc!{
                "_id": key.clone(),
            })).unwrap();
            assert!(deleted > 0, "delete nothing with key: {}", key);
            let find_doc = doc! {
                "_id": key.clone(),
            };
            let result = collection.find(&find_doc).unwrap();
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
        let mut db = prepare_db("test-very-large-data").unwrap();
        let mut collection = db.create_collection("test").unwrap();

        let mut doc = Document::new_without_id();
        let origin_data = data.clone();
        doc.insert("content".into(), Value::from(data));

        assert!(collection.insert(&mut doc).unwrap());

        let new_id = doc.pkey_id().unwrap();
        let back = collection.find_one(&doc! {
            "_id": new_id,
        }).unwrap().unwrap();

        let back_bin = back.get("content").unwrap();
        assert_eq!(back_bin.unwrap_binary().as_ref(), &origin_data);
    }

}
