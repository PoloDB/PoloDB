use polodb_core::{DbContext, DbErr, DbHandle, TransactionType};
use polodb_bson::{Value, ObjectId, Document, Array, UTCDateTime};
use polodb_bson::linked_hash_map::Iter;
use std::cell::RefCell;
use std::rc::Rc;
use std::os::raw::{c_char, c_uint, c_int, c_double, c_uchar, c_longlong};
use std::ptr::{null_mut, write_bytes, null};
use std::ffi::{CStr, CString};
use std::borrow::Borrow;

const DB_ERROR_MSG_SIZE: usize = 512;

thread_local! {
    static DB_GLOBAL_ERROR: RefCell<Option<DbErr>> = RefCell::new(None);
    static DB_GLOBAL_ERROR_MSG: RefCell<[c_char; DB_ERROR_MSG_SIZE]> = RefCell::new([0; DB_ERROR_MSG_SIZE]);
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
pub extern "C" fn PLDB_open(path: *const c_char) -> *mut DbContext {
    let cstr = unsafe {
        CStr::from_ptr(path)
    };
    let str = try_read_utf8!(cstr.to_str(), null_mut());
    let db = match DbContext::new(str.as_ref()) {
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
pub extern "C" fn PLDB_start_transaction(db: *mut DbContext, flags: c_int) -> c_int {
    unsafe {
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
}

#[no_mangle]
pub extern "C" fn PLDB_rollback(db: *mut DbContext) -> c_int {
    unsafe {
        let rust_db = db.as_mut().unwrap();
        match rust_db.rollback() {
            Ok(()) => 0,
            Err(err) => {
                set_global_error(err);
                PLDB_error_code()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn PLDB_commit(db: *mut DbContext) -> c_int {
    unsafe {
        let rust_db = db.as_mut().unwrap();
        match rust_db.commit() {
            Ok(()) => 0,
            Err(err) => {
                set_global_error(err);
                PLDB_error_code()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn PLDB_create_collection(db: *mut DbContext, name: *const c_char) -> c_int {
    unsafe {
        let name_str= CStr::from_ptr(name);
        let name_utf8 = try_read_utf8!(name_str.to_str(), PLDB_error_code());
        let oid_result = db.as_mut().unwrap().create_collection(name_utf8);
        if let Err(err) = oid_result {
            set_global_error(err);
            return PLDB_error_code();
        }
    }
    0
}

#[no_mangle]
pub extern "C" fn PLDB_insert(db: *mut DbContext, name: *const c_char, doc: *const Rc<Document>) -> c_int {
    unsafe {
        let local_db = db.as_mut().unwrap();
        let name_str = CStr::from_ptr(name);
        let name_utf8 = try_read_utf8!(name_str.to_str(), PLDB_error_code());
        let local_doc = doc.as_ref().unwrap().clone();
        let insert_result = local_db.insert(name_utf8, local_doc);
        if let Err(err) = insert_result {
            set_global_error(err);
            return PLDB_error_code();
        }
    }
    0
}

/// query is nullable
#[no_mangle]
pub extern "C" fn PLDB_find(db: *mut DbContext,
                            name: *const c_char,
                            query: *const Rc<Document>,
                            out_handle: *mut *mut DbHandle) -> c_int {
    unsafe {
        let rust_db = db.as_mut().unwrap();
        let name_str = CStr::from_ptr(name);
        let name_utf8 = try_read_utf8!(name_str.to_str(), PLDB_error_code());

        let handle_result = match query.as_ref() {
            Some(query_doc) => rust_db.find(name_utf8, Some(query_doc.borrow())),
            None => rust_db.find(name_utf8, None),
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
    }

    0
}

/// query is nullable
#[no_mangle]
pub extern "C" fn PLDB_update(db: *mut DbContext,
                              name: *const c_char,
                              query: *const Rc<Document>,
                              update: *const Rc<Document>) -> c_longlong {
    let result = unsafe {
        let rust_db = db.as_mut().unwrap();
        let name_str = CStr::from_ptr(name);
        let name_utf8 = try_read_utf8!(name_str.to_str(), PLDB_error_code() as c_longlong);

        let update_doc = update.as_ref().unwrap();

        match query.as_ref() {
            Some(query) => rust_db.update(name_utf8, Some(query.as_ref()), update_doc),
            None => rust_db.update(name_utf8, None, update_doc),
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
pub extern "C" fn PLDB_delete(db: *mut DbContext, name: *const c_char, query: *const Rc<Document>) -> c_longlong {
    let result = unsafe {
        let rust_db = db.as_mut().unwrap();
        let name_str = CStr::from_ptr(name);
        let name_utf8 = try_read_utf8!(name_str.to_str(), PLDB_error_code() as c_longlong);

        let query_doc = query.as_ref().unwrap();

        rust_db.delete(name_utf8, query_doc.as_ref())
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
pub extern "C" fn PLDB_delete_all(db: *mut DbContext, name: *const c_char) -> c_longlong {
    let result = unsafe {
        let rust_db = db.as_mut().unwrap();
        let name_str = CStr::from_ptr(name);
        let name_utf8 = try_read_utf8!(name_str.to_str(), PLDB_error_code() as c_longlong);
        rust_db.delete_all(name_utf8)
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
pub extern "C" fn PLDB_handle_to_str(handle: *mut DbHandle, buffer: *mut c_char, buffer_size: c_uint) -> c_int {
    unsafe {
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
}

#[no_mangle]
pub extern "C" fn PLDB_handle_step(handle: *mut DbHandle) -> c_int {
    unsafe {
        let rust_handle = handle.as_mut().unwrap();
        let result = rust_handle.step();

        if let Err(err) = result {
            set_global_error(err);
            return PLDB_error_code();
        }

        0
    }
}

#[no_mangle]
pub extern "C" fn PLDB_handle_state(handle: *mut DbHandle) -> c_int {
    unsafe {
        let rust_handle = handle.as_mut().unwrap();
        rust_handle.state() as c_int
    }
}

#[no_mangle]
pub extern "C" fn PLDB_handle_get(handle: *mut DbHandle, out_val: *mut *mut Value) {
    unsafe {
        let rust_handle = handle.as_mut().unwrap();
        let boxed_handle = Box::new(rust_handle.get().clone());
        let handle_ptr = Box::into_raw(boxed_handle);
        out_val.write(handle_ptr);
    }
}

#[no_mangle]
pub extern "C" fn PLDB_free_handle(handle: *mut DbHandle) {
    unsafe {
        let _ptr = Box::from_raw(handle);
    }
}

#[no_mangle]
pub extern "C" fn PLDB_error_code() -> c_int {
    return DB_GLOBAL_ERROR.with(|f| {
        if let Some(err) = f.borrow().as_ref() {
            let code = error_code_of_db_err(err) * -1;
            return code
        }
        0
    });
}

#[no_mangle]
pub extern "C" fn PLDB_error_msg() -> *const c_char {
    unsafe {
        return DB_GLOBAL_ERROR.with(|f| {
            if let Some(err) = f.borrow_mut().as_ref() {
                return DB_GLOBAL_ERROR_MSG.with(|msg| {
                    write_bytes(msg.borrow_mut().as_mut_ptr(), 0, DB_ERROR_MSG_SIZE);
                    let err_msg = err.to_string();
                    let str_size = err_msg.len();
                    let err_cstring = CString::new(err_msg).unwrap();
                    let expected_size: usize = std::cmp::min(str_size, DB_ERROR_MSG_SIZE - 1);
                    err_cstring.as_ptr().copy_to(msg.borrow_mut().as_mut_ptr(), expected_size);

                    return msg.borrow().as_ptr();
                });
            }

            return null();
        });
    }
}

#[no_mangle]
pub extern "C" fn PLDB_version(buffer: *mut c_char, buffer_size: c_uint) -> c_uint {
    let version_str = DbContext::get_version();
    let str_size = version_str.len();
    let cstring = CString::new(version_str).unwrap();
    unsafe {
        let c_ptr = cstring.as_ptr();
        let expected_size: usize = std::cmp::min(str_size, buffer_size as usize);
        c_ptr.copy_to(buffer, expected_size);
        expected_size as c_uint
    }
}

#[no_mangle]
pub extern "C" fn PLDB_close(db: *mut DbContext) {
    let _ptr = unsafe { Box::from_raw(db) };
}

#[no_mangle]
pub extern "C" fn PLDB_mk_null() -> *mut Value {
    let val = Box::new(Value::Null);
    Box::into_raw(val)
}

#[no_mangle]
pub extern "C" fn PLDB_mk_double(val: c_double) -> *mut Value {
    let val = Box::new(Value::Double(val));
    Box::into_raw(val)
}

#[no_mangle]
pub extern "C" fn PLDB_mk_bool(val: bool) -> *mut Value {
    let val = Box::new(Value::Boolean(val));
    Box::into_raw(val)
}

#[no_mangle]
pub extern "C" fn PLDB_mk_int(val: i64) -> *mut Value {
    let val = Box::new(Value::Int(val));
    Box::into_raw(val)
}

#[no_mangle]
pub extern "C" fn PLDB_value_type(val: *const Value) -> c_int {
    unsafe {
        let local_val = val.as_ref().unwrap();
        let ty = local_val.ty_int();

        ty as c_int
    }
}

#[no_mangle]
pub extern "C" fn PLDB_value_get_i64(val: *const Value, out_val: *mut i64) -> c_int {
    unsafe {
        let local_val = val.as_ref().unwrap();
        match local_val {
            Value::Int(i) => {
                out_val.write(*i);
                0
            }

            _ => -1

        }
    }
}

#[no_mangle]
pub extern "C" fn PLDB_value_get_bool(val: *const Value) -> c_int {
    unsafe {
        let local_val = val.as_ref().unwrap();
        match local_val {
            Value::Boolean(bl) =>{
                if *bl {
                    1
                } else {
                    0
                }
            }

            _ => -1,

        }
    }
}

#[no_mangle]
pub extern "C" fn PLDB_value_get_double(val: *const Value, out: *mut f64) -> c_int {
    unsafe {
        let local_val = val.as_ref().unwrap();
        match local_val {
            Value::Double(num) => {
                out.write(*num);
                0
            }

            _ => -1,

        }
    }
}

#[no_mangle]
pub extern "C" fn PLDB_value_get_array(val: *const Value, out: *mut *mut Rc<Array>) -> c_int {
    unsafe {
        let local_val = val.as_ref().unwrap();
        match local_val {
            Value::Array(arr) => {
                let boxed_array = Box::new(arr.clone());
                out.write(Box::into_raw(boxed_array));
                0
            }

            _ => -1,

        }
    }
}

#[no_mangle]
pub extern "C" fn PLDB_value_get_object_id(val: *const Value, out: *mut *mut ObjectId) -> c_int {
    unsafe {
        let local_val = val.as_ref().unwrap();
        match local_val {
            Value::ObjectId(oid) => {
                let boxed_oid: Box<ObjectId> = Box::new(oid.as_ref().clone());
                out.write(Box::into_raw(boxed_oid));
                0
            }

            _ => -1,

        }
    }
}

#[no_mangle]
pub extern "C" fn PLDB_value_get_document(val: *const Value, out: *mut *mut Rc<Document>) -> c_int {
    unsafe {
        let local_val = val.as_ref().unwrap();
        match local_val {
            Value::Document(doc) => {
                let boxed_doc = Box::new(doc.clone());
                out.write(Box::into_raw(boxed_doc));
                0
            }

            _ => -1,

        }
    }
}

#[no_mangle]
pub extern "C" fn PLDB_value_get_string_utf8(val: *const Value, out_str: *mut *const c_char) -> c_int {
    unsafe {
        let local_val = val.as_ref().unwrap();
        match local_val {
            Value::String(str) => {
                let len = str.len();
                let str_ptr = str.as_ptr().cast::<c_char>();

                out_str.write(str_ptr);

                len as c_int
            }

            _ => -1,
        }
    }
}

#[no_mangle]
pub extern "C" fn PLDB_value_get_utc_datetime(val: *const Value, out_time: *mut *mut UTCDateTime) -> c_int {
    unsafe {
        let local_val = val.as_ref().unwrap();
        match local_val {
            Value::UTCDateTime(dt) => {
                let boxed_time = Box::new(dt.as_ref().clone());
                out_time.write(Box::into_raw(boxed_time));
                0
            }

            _ => -1,
        }
    }
}

#[no_mangle]
pub extern "C" fn PLDB_mk_str(str: *const c_char) -> *mut Value {
    let str = unsafe { CStr::from_ptr(str) };
    let rust_str = try_read_utf8!(str.to_str(), null_mut());
    let val = Box::new(Value::String(Rc::new(rust_str.to_string())));
    Box::into_raw(val)
}

#[no_mangle]
pub extern "C" fn PLDB_mk_arr() -> *mut Rc<Array> {
    let result = Box::new(Rc::new(Array::new()));
    Box::into_raw(result)
}

#[no_mangle]
pub extern "C" fn PLDB_free_arr(arr: *mut Rc<Array>) {
    let _ptr = unsafe { Box::from_raw(arr) };
}

#[no_mangle]
pub extern "C" fn PLDB_arr_to_value(arr: *mut Rc<Array>) -> *mut Value {
    let boxed_value = unsafe {
        let local_arr = arr.as_ref().unwrap();
        let local_value = Value::Array(local_arr.clone());
        Box::new(local_value)
    };
    Box::into_raw(boxed_value)
}

#[no_mangle]
pub extern "C" fn PLDB_arr_push(arr: *mut Rc<Array>, val: *const Value) {
    unsafe {
        let local_arr = arr.as_mut().unwrap();
        let arr_mut = Rc::get_mut(local_arr).unwrap();
        let local_val = val.as_ref().unwrap();
        arr_mut.push(local_val.clone())
    }
}

#[no_mangle]
pub extern "C" fn PLDB_arr_get(arr: *mut Rc<Array>, index: c_uint, out_val: *mut *mut Value) -> c_int {
    unsafe {
        let local_arr = arr.as_mut().unwrap();
        let val = &local_arr[index as usize];
        let out_box = Box::new(val.clone());
        out_val.write(Box::into_raw(out_box));
        0
    }

}

#[no_mangle]
pub extern "C" fn PLDB_arr_len(arr: *mut Rc<Array>) -> c_uint {
    unsafe {
        let local_arr = arr.as_ref().unwrap();
        local_arr.len()
    }
}

#[no_mangle]
pub extern "C" fn PLDB_mk_doc() -> *mut Rc<Document> {
    let result = Box::new(Rc::new(Document::new_without_id()));
    Box::into_raw(result)
}

#[no_mangle]
pub extern "C" fn PLDB_doc_set(doc: *mut Rc<Document>, key: *const c_char, value: *const Value) -> c_int {
    unsafe {
        let mut local_doc = doc.as_mut().unwrap();
        let key_str = CStr::from_ptr(key);
        let local_value = value.as_ref().unwrap();
        let key = try_read_utf8!(key_str.to_str(), PLDB_error_code());
        let local_doc_mut = Rc::get_mut(&mut local_doc).unwrap();
        let result = local_doc_mut.insert(key.to_string(), local_value.clone());
        if let Some(_) = result {
            1
        } else {
            0
        }
    }
}

#[no_mangle]
pub extern "C" fn PLDB_doc_get(doc: *mut Rc<Document>, key: *const c_char, result: *mut *mut Value) -> c_int {
    unsafe {
        let local_doc = doc.as_mut().unwrap();
        let key_str = CStr::from_ptr(key);
        let utf8_key = try_read_utf8!(key_str.to_str(), PLDB_error_code());
        let get_result = local_doc.get(utf8_key);
        if let Some(value) = get_result {
            let out_box = Box::new(value.clone());
            result.write(Box::into_raw(out_box));
            return 1;
        }
        return 0;
    }
}

#[no_mangle]
pub extern "C" fn PLDB_doc_len(doc: *mut Rc<Document>) -> c_int {
    unsafe {
        let local_doc = doc.as_mut().unwrap();
        let len = local_doc.len();
        len as c_int
    }
}

#[no_mangle]
pub extern "C" fn PLDB_doc_iter(doc: *mut Rc<Document>) -> *mut Iter<'static, String, Value> {
    unsafe {
        let local_doc = doc.as_mut().unwrap();
        let iter = local_doc.iter();
        Box::into_raw(Box::new(iter))
    }
}

#[no_mangle]
pub extern "C" fn PLDB_doc_iter_next(iter: *mut Iter<'static, String, Value>,
                                     key_buffer: *mut c_char, key_buffer_size: c_uint, out_val: *mut *mut Value) -> c_int {

    unsafe {
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

                let cstr = CString::new(key.clone()).unwrap();
                cstr.as_ptr().copy_to_nonoverlapping(key_buffer, real_size);

                let boxed_value = Box::new(value.clone());
                out_val.write(Box::into_raw(boxed_value));
                real_size as c_int
            }

            None => {
                0
            }

        }
    }
}

#[no_mangle]
pub extern "C" fn PLDB_free_doc_iter(iter: *mut Iter<'static, String, Value>) {
    unsafe {
        let _ptr = Box::from_raw(iter);
    }
}

#[no_mangle]
pub extern "C" fn PLDB_free_doc(doc: *mut Rc<Document>) {
    unsafe {
        let _ptr = Box::from_raw(doc);
    }
}

#[no_mangle]
pub extern "C" fn PLDB_mk_UTCDateTime(time: i64) -> *mut UTCDateTime {
    let datetime = if time >= 0 {
        UTCDateTime::new(time as u64)
    } else {
        UTCDateTime::now()
    };

    let boxed_datetime = Box::new(datetime);
    Box::into_raw(boxed_datetime)
}

#[no_mangle]
pub extern "C" fn PLDB_UTCDateTime_get_timestamp(dt: *const UTCDateTime) -> i64 {
    unsafe {
        let dt = dt.as_ref().unwrap();
        dt.timestamp() as i64
    }
}

#[no_mangle]
pub extern "C" fn PLDB_UTCDateTime_to_value(dt: *const UTCDateTime) -> *mut Value {
    let doc: UTCDateTime = unsafe {
        dt.as_ref().unwrap().clone()
    };

    let val = Box::new(Value::from(doc));
    Box::into_raw(val)
}

#[no_mangle]
pub extern "C" fn PLDB_free_UTCDateTime(dt: *mut UTCDateTime) {
    unsafe {
        let _ = Box::from_raw(dt);
    }
}

#[no_mangle]
pub extern "C" fn PLDB_mk_binary(data: *mut c_uchar, size: c_uint) -> *mut Value {
    let mut buffer: Vec<u8> = Vec::with_capacity(size as usize);
    buffer.resize(size as usize, 0);
    unsafe {
        data.copy_to(buffer.as_mut_ptr(), size as usize);
    }
    let val = Box::new(Value::Binary(Rc::new(buffer)));
    Box::into_raw(val)
}

#[no_mangle]
pub extern "C" fn PLDB_mk_object_id(db: *mut DbContext) -> *mut ObjectId {
    let rust_db = unsafe { db.as_mut().unwrap() };
    let oid = rust_db.object_id_maker().mk_object_id();
    let oid = Box::new(oid);
    Box::into_raw(oid)
}

#[no_mangle]
pub extern "C" fn PLDB_free_object_id(oid: *mut ObjectId) {
    unsafe {
        let _ptr = Box::from_raw(oid);
    }
}

#[no_mangle]
pub extern "C" fn PLDB_object_id_to_value(oid: *const ObjectId) -> *mut Value {
    unsafe {
        let rust_oid = oid.as_ref().unwrap();
        let value: Value = rust_oid.clone().into();
        let boxed_value = Box::new(value);
        Box::into_raw(boxed_value)
    }
}

#[no_mangle]
pub extern "C" fn PLDB_doc_to_value(oid: *const Rc<Document>) -> *mut Value {
    unsafe {
        let rust_doc = oid.as_ref().unwrap();
        let value = Value::Document(rust_doc.clone());
        let boxed_value = Box::new(value);
        Box::into_raw(boxed_value)
    }
}

#[no_mangle]
pub extern "C" fn PLDB_object_id_to_hex(oid: *const ObjectId, buffer: *mut c_char, buffer_size: c_uint) -> c_int {
    unsafe  {
        let rust_oid = oid.as_ref().unwrap();
        let oid_hex = rust_oid.to_hex();
        let size = oid_hex.len();
        let cstr = CString::new(oid_hex).unwrap();
        let real_size = std::cmp::min(size, buffer_size as usize);
        cstr.as_ptr().copy_to_nonoverlapping(buffer, real_size);
        real_size as c_int
    }
}

#[no_mangle]
pub extern "C" fn PLDB_free_value(val: *mut Value) {
    let _val = unsafe { Box::from_raw(val) };
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
        DbErr::MetaPageIdError => 25,
        DbErr::CannotWriteDbWithoutTransaction => 26,
        DbErr::StartTransactionInAnotherTransaction => 27,
        DbErr::RollbackNotInTransaction => 28,
        DbErr::IllegalCollectionName(_) => 29,
        DbErr::UnexpectedHeaderForBtreePage => 30,
        DbErr::KeyTypeOfBtreeShouldNotBeZero => 31,
        DbErr::UnexpectedPageHeader => 32,
        DbErr::UnexpectedPageType => 33,
        DbErr::UnknownTransactionType => 34,
        DbErr::BufferNotEnough(_) => 35,
        DbErr::UnknownUpdateOperation(_) => 36,
        DbErr::IncrementNullField => 37,
        DbErr::VmIsHalt => 38,
        DbErr::Busy => 39,

    }
}
