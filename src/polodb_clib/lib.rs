#![allow(clippy::missing_safety_doc)]

use polodb_core::{DbContext, DbErr, DbHandle, TransactionType, Config};
use polodb_bson::{Value, ObjectId, Document, Array, UTCDateTime, ty_int};
use polodb_bson::linked_hash_map::Iter;
use std::cell::RefCell;
use std::rc::Rc;
use std::os::raw::{c_char, c_uint, c_int, c_double, c_longlong};
use std::ptr::{null_mut, write_bytes, null};
use std::ffi::{CStr, CString};
use std::borrow::Borrow;

const DB_ERROR_MSG_SIZE: usize = 512;

thread_local! {
    static DB_GLOBAL_ERROR: RefCell<Option<DbErr>> = RefCell::new(None);
    static DB_GLOBAL_ERROR_MSG: RefCell<[c_char; DB_ERROR_MSG_SIZE]> = RefCell::new([0; DB_ERROR_MSG_SIZE]);
}

#[repr(C)]
pub union ValueUnion {
    int_value: i64,
    double_value: c_double,
    bool_value: c_int,
    str: *mut c_char,
    oid: *mut ObjectId,
    arr: *mut Rc<Array>,
    doc: *mut Rc<Document>,
    bin: *mut Rc<Vec<u8>>,
    utc: u64,
}

#[repr(C)]
pub struct ValueMock {
    tag:   u8,
    value: ValueUnion,
}

macro_rules! try_read_utf8 {
    ($action:expr, $ret:expr) => {
        match $action {
            Ok(str) => str,
            Err(err) => {
                set_global_error(err.into());
                return $ret;
            }
        }
    }
}

fn set_global_error(err: DbErr) {
    DB_GLOBAL_ERROR.with(|f| {
        *f.borrow_mut() = Some(err);
    });
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_open(path: *const c_char) -> *mut DbContext {
    let cstr = CStr::from_ptr(path);
    let str = try_read_utf8!(cstr.to_str(), null_mut());
    let db = match DbContext::new(str.as_ref(), Config::default()) {
        Ok(db) => db,
        Err(err) => {
            set_global_error(err);
            return null_mut();
        }
    };
    let ptr = Box::new(db);
    Box::into_raw(ptr)
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_start_transaction(db: *mut DbContext, flags: c_int) -> c_int {
    let rust_db = db.as_mut().unwrap();
    let ty = match flags {
        0 => None,
        1 => Some(TransactionType::Read),
        2 => Some(TransactionType::Write),
        _ => {
            set_global_error(DbErr::UnknownTransactionType);
            return PLDB_error_code();
        }
    };
    match rust_db.start_transaction(ty) {
        Ok(()) => 0,
        Err(err) => {
            set_global_error(err);
            PLDB_error_code()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_rollback(db: *mut DbContext) -> c_int {
    let rust_db = db.as_mut().unwrap();
    match rust_db.rollback() {
        Ok(()) => 0,
        Err(err) => {
            set_global_error(err);
            PLDB_error_code()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_commit(db: *mut DbContext) -> c_int {
    let rust_db = db.as_mut().unwrap();
    match rust_db.commit() {
        Ok(()) => 0,
        Err(err) => {
            set_global_error(err);
            PLDB_error_code()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_count(db: *mut DbContext, col_id: c_uint, meta_version: u32) -> c_longlong {
    let rust_db = db.as_mut().unwrap();
    let result = rust_db.count(col_id, meta_version);
    match result {
        Ok(result) => {
            result as c_longlong
        }
        Err(err) => {
            set_global_error(err);
            PLDB_error_code() as c_longlong
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_create_collection(db: *mut DbContext,
                                         name: *const c_char,
                                         col_id: *mut c_uint,
                                         meta_version: *mut c_uint) -> c_int {
    let name_str= CStr::from_ptr(name);
    let name_utf8 = try_read_utf8!(name_str.to_str(), PLDB_error_code());
    let oid_result = db.as_mut().unwrap().create_collection(name_utf8);
    match oid_result {
        Ok(meta) => {
            col_id.write(meta.id);
            meta_version.write(meta.meta_version);
            0
        }

        Err(err) => {
            set_global_error(err);
            PLDB_error_code()
        }

    }
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_drop(db: *mut DbContext, col_id: c_uint, meta_version: c_uint) -> c_int {
    let result = db.as_mut().unwrap().drop(col_id, meta_version);
    if let Err(err) = result {
        set_global_error(err);
        return PLDB_error_code();
    }
    0
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_get_collection_meta_by_name(db: *mut DbContext, name: *const c_char, id: *mut c_uint, version: *mut c_uint) -> c_int {
    let str = CStr::from_ptr(name);
    let utf8str = try_read_utf8!(str.to_str(), PLDB_error_code());
    let result = db.as_mut().unwrap().get_collection_meta_by_name(utf8str);
    match result {
        Ok(info) => {
            id.write(info.id);
            version.write(info.meta_version);
            0
        }

        Err(err) => {
            set_global_error(err);
            PLDB_error_code()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_insert(db: *mut DbContext, col_id: c_uint, meta_version: c_uint, doc: *mut Rc<Document>) -> c_int {
    let local_db = db.as_mut().unwrap();
    let local_doc = doc.as_mut().unwrap();
    let mut_doc = Rc::make_mut(local_doc);
    let insert_result = local_db.insert(col_id, meta_version, mut_doc);
    if let Err(err) = insert_result {
        set_global_error(err);
        return PLDB_error_code();
    }
    match insert_result {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(err) => {
            set_global_error(err);
            PLDB_error_code()
        }
    }
}

/// query is nullable
#[no_mangle]
pub unsafe extern "C" fn PLDB_find(db: *mut DbContext,
                            col_id: c_uint,
                            meta_version: c_uint,
                            query: *const Rc<Document>,
                            out_handle: *mut *mut DbHandle) -> c_int {
    let rust_db = db.as_mut().unwrap();

    let handle_result = match query.as_ref() {
        Some(query_doc) => rust_db.find(col_id, meta_version, Some(query_doc.borrow())),
        None => rust_db.find(col_id, meta_version, None),
    };

    let handle = match handle_result {
        Ok(handle) => handle,
        Err(err) => {
            set_global_error(err);
            return PLDB_error_code();
        }
    };

    let boxed_handle = Box::new(handle);
    let raw_handle = Box::into_raw(boxed_handle);

    out_handle.write(raw_handle);

    0
}

/// query is nullable
#[no_mangle]
pub unsafe extern "C" fn PLDB_update(db: *mut DbContext,
                              col_id: c_uint,
                              meta_version: c_uint,
                              query: *const Rc<Document>,
                              update: *const Rc<Document>) -> c_longlong {
    let result = {
        let rust_db = db.as_mut().unwrap();

        let update_doc = update.as_ref().unwrap();

        match query.as_ref() {
            Some(query) => rust_db.update(col_id, meta_version, Some(query.as_ref()), update_doc),
            None => rust_db.update(col_id, meta_version, None, update_doc),
        }
    };

    match result {
        Ok(result) => result as c_longlong,
        Err(err) => {
            set_global_error(err);
            PLDB_error_code() as c_longlong
        }
    }
}

/// return value represents how many rows are deleted
#[no_mangle]
pub unsafe extern "C" fn PLDB_delete(db: *mut DbContext, col_id: c_uint, meta_version: c_uint, query: *const Rc<Document>) -> c_longlong {
    let rust_db = db.as_mut().unwrap();
    let query_doc = query.as_ref().unwrap();
    let result = rust_db.delete(col_id, meta_version, query_doc.as_ref());

    match result {
        Ok(size) => size as c_longlong,
        Err(err) => {
            set_global_error(err);
            PLDB_error_code() as c_longlong
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_delete_all(db: *mut DbContext, col_id: c_uint, meta_version: c_uint) -> c_longlong {
    let result = {
        let rust_db = db.as_mut().unwrap();
        rust_db.delete_all(col_id, meta_version)
    };

    match result {
        Ok(size) => size as c_longlong,
        Err(err) => {
            set_global_error(err);
            PLDB_error_code() as c_longlong
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_handle_to_str(handle: *mut DbHandle, buffer: *mut c_char, buffer_size: c_uint) -> c_int {
    let rust_handle = handle.as_mut().unwrap();
    let str_content = format!("{}", rust_handle);
    let length = str_content.len();

    if buffer.is_null() {
        return (length + 1) as c_int;
    }

    if (buffer_size as usize) < length + 1 {
        set_global_error(DbErr::BufferNotEnough(length + 1));
        return PLDB_error_code();
    }

    let cstring = CString::new(str_content).unwrap();
    cstring.as_ptr().copy_to_nonoverlapping(buffer, length);

    length as c_int
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_step(handle: *mut DbHandle) -> c_int {
    let rust_handle = handle.as_mut().unwrap();
    let result = rust_handle.step();

    if let Err(err) = result {
        set_global_error(err);
        return PLDB_error_code();
    }

    0
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_handle_state(handle: *mut DbHandle) -> c_int {
    let rust_handle = handle.as_mut().unwrap();
    rust_handle.state() as c_int
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_handle_get(handle: *mut DbHandle, out_val: *mut ValueMock) {
    let rust_handle = handle.as_mut().unwrap();
    let mock = db_value_to_mock_value(rust_handle.get());
    out_val.write(mock);
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_close_and_free_handle(handle: *mut DbHandle) {
    let handle = Box::from_raw(handle);
    if let Err(err) = handle.commit_and_close_vm() {
        set_global_error(err);
    }
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_free_handle(handle: *mut DbHandle) {
    let _ptr = Box::from_raw(handle);
}

#[no_mangle]
pub extern "C" fn PLDB_error_code() -> c_int {
    DB_GLOBAL_ERROR.with(|f| {
        if let Some(err) = f.borrow().as_ref() {
            let code = error_code_of_db_err(err) * -1;
            return code
        }
        0
    })
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_error_msg() -> *const c_char {
    DB_GLOBAL_ERROR.with(|f| {
        if let Some(err) = f.borrow_mut().as_ref() {
            return DB_GLOBAL_ERROR_MSG.with(|msg| {
                write_bytes(msg.borrow_mut().as_mut_ptr(), 0, DB_ERROR_MSG_SIZE);
                let err_msg = err.to_string();
                let str_size = err_msg.len();
                let err_cstring = CString::new(err_msg).unwrap();
                let expected_size: usize = std::cmp::min(str_size, DB_ERROR_MSG_SIZE - 1);
                err_cstring.as_ptr().copy_to(msg.borrow_mut().as_mut_ptr(), expected_size);

                msg.borrow().as_ptr()
            });
        }

        null()
    })
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_version(buffer: *mut c_char, buffer_size: c_uint) -> c_uint {
    let version_str = DbContext::get_version();
    let str_size = version_str.len();
    let cstring = CString::new(version_str).unwrap();
    let c_ptr = cstring.as_ptr();
    let expected_size: usize = std::cmp::min(str_size, buffer_size as usize);
    c_ptr.copy_to(buffer, expected_size);
    expected_size as c_uint
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_close(db: *mut DbContext) {
    let _ptr = Box::from_raw(db);
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_mk_binary_value(ptr: *const c_char, size: u32) -> ValueMock {
    let mut bytes: Vec<u8> = Vec::new();
    bytes.resize(size as usize, 0);
    ptr.copy_to(bytes.as_mut_ptr().cast(), size as usize);
    let raw: *mut Rc<Vec<u8>> = Box::into_raw(Box::new(Rc::new(bytes)));
    ValueMock {
        tag: ty_int::BINARY,
        value: ValueUnion {
            bin: raw,
        },
    }
}

#[no_mangle]
pub extern "C" fn PLDB_mk_arr() -> *mut Rc<Array> {
    let result = Box::new(Rc::new(Array::new()));
    Box::into_raw(result)
}

#[no_mangle]
pub extern "C" fn PLDB_mk_arr_with_size(size: c_uint) -> *mut Rc<Array> {
    let result = Box::new(Rc::new(Array::new_with_size(size as usize)));
    Box::into_raw(result)
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_free_arr(arr: *mut Rc<Array>) {
    let _ptr = Box::from_raw(arr);
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_arr_push(arr: *mut Rc<Array>, val: ValueMock) {
    let local_arr = arr.as_mut().unwrap();
    let arr_mut = Rc::get_mut(local_arr).unwrap();
    let local_value = mock_value_to_db_value(val).unwrap();
    arr_mut.push(local_value)
}

unsafe fn db_value_to_mock_value(value: &Value) -> ValueMock {
    match value {
        Value::Null => {
            ValueMock {
                tag: ty_int::NULL,
                value: ValueUnion {
                    int_value: 0,
                },
            }
        }

        Value::Int(i) => {
            ValueMock {
                tag: ty_int::INT,
                value: ValueUnion {
                    int_value: *i,
                },
            }
        }

        Value::Double(db) => {
            ValueMock {
                tag: ty_int::DOUBLE,
                value: ValueUnion {
                    double_value: *db,
                },
            }
        }

        Value::Boolean(bl) => {
            ValueMock {
                tag: ty_int::BOOLEAN,
                value: ValueUnion {
                    bool_value: if *bl {
                        1
                    } else {
                        0
                    },
                },
            }
        }

        Value::String(str) => {
            let len = str.len();
            let bytes: *mut c_char = libc::malloc(len + 1).cast();
            libc::memset(bytes.cast(), 0, len + 1);
            str.as_ptr().copy_to(bytes.cast(), len);
            ValueMock {
                tag: ty_int::STRING,
                value: ValueUnion {
                    str: bytes,
                },
            }
        }

        Value::Binary(arr) => {
            let bin: *mut Rc<Vec<u8>> = Box::into_raw(Box::new(arr.clone()));
            ValueMock {
                tag: ty_int::BINARY,
                value: ValueUnion {
                    bin,
                },
            }
        }

        Value::Document(doc) => {
            let d: *mut Rc<Document> = Box::into_raw(Box::new(doc.clone()));
            ValueMock {
                tag: ty_int::DOCUMENT,
                value: ValueUnion {
                    doc: d,
                },
            }
        }

        Value::Array(arr) => {
            let a: *mut Rc<Array> = Box::into_raw(Box::new(arr.clone()));
            ValueMock {
                tag: ty_int::ARRAY,
                value: ValueUnion {
                    arr: a,
                },
            }
        }

        Value::ObjectId(oid) =>{
            let a: *mut ObjectId = Box::into_raw(Box::new(oid.as_ref().clone()));
            ValueMock {
                tag: ty_int::OBJECT_ID,
                value: ValueUnion {
                    oid: a,
                },
            }
        }

        Value::UTCDateTime(dt) => {
            ValueMock {
                tag: ty_int::UTC_DATETIME,
                value: ValueUnion {
                    utc: dt.timestamp(),
                },
            }
        }

    }
}

unsafe fn mock_value_to_db_value(v: ValueMock) -> Option<Value> {
    match v.tag {
        ty_int::NULL => {
            Some(Value::Null)
        }

        ty_int::DOUBLE => {
            Some(Value::from(v.value.double_value))
        }

        ty_int::BOOLEAN => {
            Some(Value::Boolean(v.value.bool_value != 0))
        }

        ty_int::INT => {
            Some(Value::Int(v.value.int_value))
        }

        ty_int::STRING => {
            let local_str = CStr::from_ptr(v.value.str);
            let utf8 = local_str.to_str().unwrap();
            Some(Value::from(utf8))
        }

        ty_int::OBJECT_ID => {
            let oid_ref = v.value.oid.as_ref().unwrap();
            Some(Value::ObjectId(Rc::new(oid_ref.clone())))
        }

        ty_int::ARRAY => {
            let local_ref = v.value.arr.as_ref().unwrap();
            Some(Value::Array(local_ref.clone()))
        }

        ty_int::DOCUMENT => {
            let local_ref = v.value.doc.as_ref().unwrap();
            Some(Value::Document(local_ref.clone()))
        }

        ty_int::BINARY => {
            let local_bin = v.value.bin.as_ref().unwrap();
            Some(Value::Binary(local_bin.clone()))
        }

        ty_int::UTC_DATETIME => {
            Some(Value::UTCDateTime(Rc::new(UTCDateTime::new(v.value.utc))))
        }

        _ => None,

    }
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_arr_get(arr: *mut Rc<Array>, index: c_uint, out_val: *mut ValueMock) -> c_int {
    let local_arr = arr.as_mut().unwrap();
    let val = &local_arr[index as usize];
    let mock = db_value_to_mock_value(val);
    out_val.write(mock);
    0
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_arr_len(arr: *mut Rc<Array>) -> c_uint {
    let local_arr = arr.as_ref().unwrap();
    local_arr.len()
}

#[no_mangle]
pub extern "C" fn PLDB_mk_doc() -> *mut Rc<Document> {
    let result = Box::new(Rc::new(Document::new_without_id()));
    Box::into_raw(result)
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_doc_set(doc: *mut Rc<Document>, key: *const c_char, value: ValueMock) -> c_int {
    let local_doc = doc.as_mut().unwrap();
    let key_str = CStr::from_ptr(key);
    let key = try_read_utf8!(key_str.to_str(), PLDB_error_code());
    let local_doc_mut = Rc::get_mut(local_doc).unwrap();
    let v = mock_value_to_db_value(value).unwrap();
    let result = local_doc_mut.insert(key.into(), v);
    if result.is_some() {
        1
    } else {
        0
    }
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_arr_set(arr: *mut Rc<Array>, index: u32, value: ValueMock) -> c_int {
    let local_arr = arr.as_mut().unwrap();
    let local_arr_mut = Rc::get_mut(local_arr).unwrap();
    let result = mock_value_to_db_value(value).unwrap();
    local_arr_mut[index as usize] = result;
    0
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_doc_get(doc: *mut Rc<Document>, key: *const c_char, result: *mut ValueMock) -> c_int {
    let local_doc = doc.as_mut().unwrap();
    let key_str = CStr::from_ptr(key);
    let utf8_key = try_read_utf8!(key_str.to_str(), PLDB_error_code());
    let get_result = local_doc.get(utf8_key);
    if let Some(value) = get_result {
        let value_mock = db_value_to_mock_value(value);
        result.write(value_mock);
        return 1;
    }
    0
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_doc_len(doc: *mut Rc<Document>) -> c_int {
    let local_doc = doc.as_mut().unwrap();
    let len = local_doc.len();
    len as c_int
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_doc_iter(doc: *mut Rc<Document>) -> *mut Iter<'static, String, Value> {
    let local_doc = doc.as_mut().unwrap();
    let iter = local_doc.iter();
    Box::into_raw(Box::new(iter))
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_doc_iter_next(iter: *mut Iter<'static, String, Value>,
                                     key_buffer: *mut c_char, key_buffer_size: c_uint, out_val: *mut ValueMock) -> c_int {

    let local_iter = iter.as_mut().unwrap();
    let tuple = local_iter.next();
    match tuple {
        Some((key, value)) => {
            let key_len = key.len();
            if key_len > (key_buffer_size as usize) {
                set_global_error(DbErr::BufferNotEnough(key_len));
                return PLDB_error_code();
            }
            let real_size = std::cmp::min(key_len, key_buffer_size as usize);

            let cstr = CString::new(key.as_str()).unwrap();
            cstr.as_ptr().copy_to_nonoverlapping(key_buffer, real_size);

            let value_mock = db_value_to_mock_value(value);
            out_val.write(value_mock);
            real_size as c_int
        }

        None => {
            0
        }

    }
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_free_doc_iter(iter: *mut Iter<'static, String, Value>) {
    let _ptr = Box::from_raw(iter);
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_free_doc(doc: *mut Rc<Document>) {
    let _ptr = Box::from_raw(doc);
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_mk_UTCDateTime() -> u64 {
    UTCDateTime::now().timestamp()
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_mk_object_id(db: *mut DbContext) -> *mut ObjectId {
    let rust_db = db.as_mut().unwrap();
    let oid = rust_db.object_id_maker().mk_object_id();
    let oid = Box::new(oid);
    Box::into_raw(oid)
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_dup_object_id(oid: *const ObjectId) -> *mut ObjectId {
    let oid_ref = oid.as_ref().unwrap();
    let new_oid = Box::new(oid_ref.clone());
    Box::into_raw(new_oid)
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_dup_value(val: ValueMock) -> ValueMock {
    let val = mock_value_to_db_value(val).unwrap();
    db_value_to_mock_value(&val)
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_mk_object_id_from_bytes(bytes: *const c_char) -> *mut ObjectId {
    let mut bytes_array: [u8; 12] = [0; 12];
    bytes.cast::<u8>().copy_to(bytes_array.as_mut_ptr(), 12);
    let oid_result = ObjectId::deserialize(&bytes_array);
    if let Err(err) = oid_result {
        set_global_error(DbErr::BsonErr(Box::new(err)));
        return null_mut();
    }
    let oid = Box::new(oid_result.unwrap());
    Box::into_raw(oid)
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_free_object_id(oid: *mut ObjectId) {
    let _ptr = Box::from_raw(oid);
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_object_id_to_hex(oid: *const ObjectId, buffer: *mut c_char, buffer_size: c_uint) -> c_int {
    let rust_oid = oid.as_ref().unwrap();
    let oid_hex = rust_oid.to_hex();
    let size = oid_hex.len();
    let cstr = CString::new(oid_hex).unwrap();
    let real_size = std::cmp::min(size, buffer_size as usize);
    cstr.as_ptr().copy_to_nonoverlapping(buffer, real_size);
    real_size as c_int
}

#[no_mangle]
pub unsafe extern  "C" fn PLDB_object_id_to_bytes(oid: *const ObjectId, bytes: *mut c_char) {
    let oid = oid.as_ref().unwrap();
    let mut vec: Vec<u8> = Vec::with_capacity(12);
    oid.serialize(&mut vec).unwrap();

    vec.as_ptr().copy_to(bytes.cast(), 12);
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_free_value(v: ValueMock) {
    match v.tag {
        ty_int::NULL
        | ty_int::DOUBLE

        | ty_int::BOOLEAN

        | ty_int::UTC_DATETIME

        | ty_int::INT => {
            // ignore
        }

        ty_int::STRING => {
            libc::free(v.value.str.cast());
        }

        ty_int::OBJECT_ID => {
            let _ = Box::from_raw(v.value.oid);
        }

        ty_int::ARRAY => {
            let _ = Box::from_raw(v.value.arr);
        }

        ty_int::DOCUMENT => {
            let _ = Box::from_raw(v.value.doc);
        }

        ty_int::BINARY => {
            let _ = Box::from_raw(v.value.bin);
        }

        _ => unreachable!(),

    }
}

fn error_code_of_db_err(err: &DbErr) -> i32 {
    match err {
        DbErr::UnexpectedIdType(_, _) => 1,
        DbErr::NotAValidKeyType(_) => 2,
        DbErr::ValidationError(_) => 3,
        DbErr::InvalidOrderOfIndex(_) => 4,
        DbErr::IndexAlreadyExists(_) => 5,
        DbErr::FieldTypeUnexpected(_) => 6,
        DbErr::ParseError(_) => 7,
        DbErr::IOErr(_) => 9,
        DbErr::UTF8Err(_) => 10,
        DbErr::DataSizeTooLarge(_, _) => 12,
        DbErr::DecodeEOF => 13,
        DbErr::BsonErr(_) => 14,
        DbErr::DataOverflow => 15,
        DbErr::DataExist(_) => 16,
        DbErr::PageSpaceNotEnough => 17,
        DbErr::DataHasNoPrimaryKey => 18,
        DbErr::ChecksumMismatch => 19,
        DbErr::JournalPageSizeMismatch(_, _) => 20,
        DbErr::SaltMismatch => 21,
        DbErr::PageMagicMismatch(_) => 22,
        DbErr::ItemSizeGreaterThanExpected => 23,
        DbErr::CollectionNotFound(_) => 24,
        DbErr::CollectionIdNotFound(_) => 25,
        DbErr::MetaPageIdError => 26,
        DbErr::CannotWriteDbWithoutTransaction => 27,
        DbErr::StartTransactionInAnotherTransaction => 28,
        DbErr::RollbackNotInTransaction => 29,
        DbErr::IllegalCollectionName(_) => 30,
        DbErr::UnexpectedHeaderForBtreePage(_) => 31,
        DbErr::KeyTypeOfBtreeShouldNotBeZero => 32,
        DbErr::UnexpectedPageHeader => 33,
        DbErr::UnexpectedPageType => 34,
        DbErr::UnknownTransactionType => 35,
        DbErr::BufferNotEnough(_) => 36,
        DbErr::UnknownUpdateOperation(_) => 37,
        DbErr::IncrementNullField => 38,
        DbErr::VmIsHalt => 39,
        DbErr::MetaVersionMismatched(_, _) => 40,
        DbErr::Busy => 41,
        DbErr::InvalidField(_) => 42,
        DbErr::CollectionAlreadyExits(_) => 43,
        DbErr::UnableToUpdatePrimaryKey => 44,
        DbErr::UnexpectedTypeForOp(_) => 45,
        DbErr::NotAValidDatabase => 46,
        DbErr::DatabaseOccupied => 47,
        DbErr::Multiple(_) => 48,

    }
}
