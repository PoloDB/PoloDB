/*
 * Copyright (c) 2020 Vincent Chan
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation; either version 3, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */
use std::cell::RefCell;
use std::rc::Rc;
use std::os::raw::{c_char, c_uint, c_int, c_double, c_uchar};
use std::ptr::{null_mut, write_bytes, null};
use std::ffi::{CStr, CString};
use polodb_core::{Database, DbErr};
use polodb_core::bson::{Value, ObjectId, Document, Array};

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
                set_global_error(DbErr::UTF8Err(err));
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
pub extern "C" fn PLDB_open(path: *const c_char) -> *mut Database {
    let cstr = unsafe {
        CStr::from_ptr(path)
    };
    let str = try_read_utf8!(cstr.to_str(), null_mut());
    let db = match Database::open(str) {
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
pub extern "C" fn PLDB_create_collection(db: *mut Database, name: *const c_char) -> c_int {
    unsafe {
        let name_str= CStr::from_ptr(name);
        let name_utf8 = try_read_utf8!(name_str.to_str(), PLDB_error_code());
        let oid_result = db.as_mut().unwrap().create_collection(name_utf8);
        if let Err(err) = oid_result {
            set_global_error(err);
            return -1;
        }
    }
    0
}

#[no_mangle]
pub extern "C" fn PLDB_insert(db: *mut Database, name: *const c_char, doc: *const Document) -> c_int {
    unsafe {
        let local_db = db.as_mut().unwrap();
        let name_str = CStr::from_ptr(name);
        let name_utf8 = try_read_utf8!(name_str.to_str(), PLDB_error_code());
        let local_doc: &Document = doc.as_ref().unwrap();
        let insert_result = local_db.insert(name_utf8, Rc::new(local_doc.clone()));
        if let Err(err) = insert_result {
            set_global_error(err);
            return -1;
        }
    }
    0
}

#[no_mangle]
pub extern "C" fn PLDB_find(_db: *mut Database, _val: *mut Value) -> c_int {
    println!("find");
    0
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
pub extern "C" fn PLDB_exec(_db: *mut Database, _bytes: *const u8, size: c_uint) -> c_int {
    print!("exec byte codes with size: {}", size);
    0
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
    let version_str = Database::get_version();
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
pub extern "C" fn PLDB_close(db: *mut Database) {
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
pub extern "C" fn PLDB_value_type_name(val: *const Value, buffer: *mut c_char, buffer_size: c_uint) -> c_int {
    unsafe {
        let local_val = val.as_ref().unwrap();
        let rust_name = local_val.ty_name();
        let actual_size = rust_name.len();
        let cstr = CString::new(rust_name).unwrap();
        let result_size = std::cmp::min(actual_size, buffer_size as usize);

        cstr.as_ptr().copy_to_nonoverlapping(buffer, result_size);

        result_size as c_int
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
pub extern "C" fn PLDB_arr_into_value(arr: *mut Rc<Array>) -> *mut Value {
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
pub extern "C" fn PLDB_free_doc(doc: *mut Rc<Document>) {
    unsafe {
        let _ptr = Box::from_raw(doc);
    }
}

#[no_mangle]
pub extern "C" fn PLDB_doc_into_value(doc: *mut Rc<Document>) -> *mut Value {
    let doc: Rc<Document> = unsafe {
        doc.as_ref().unwrap().clone()
    };

    let val = Box::new(Value::Document(doc));
    Box::into_raw(val)
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
pub extern "C" fn PLDB_mk_object_id(db: *mut Database) -> *mut ObjectId {
    let rust_db = unsafe { db.as_mut().unwrap() };
    let oid = Box::new(rust_db.mk_object_id());
    Box::into_raw(oid)
}

#[no_mangle]
pub extern "C" fn PLDB_free_object_id(oid: *mut ObjectId) {
    unsafe {
        let _ptr = Box::from_raw(oid);
    }
}

#[no_mangle]
pub extern "C" fn PLDB_object_id_into_value(oid: *const ObjectId) -> *mut Value {
    unsafe {
        let rust_oid = oid.as_ref().unwrap();
        let value = Value::ObjectId(Rc::new(rust_oid.clone()));
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
        DbErr::IndexOptionsTypeUnexpected(_) => 6,
        DbErr::ParseError(_) => 7,
        DbErr::ParseIntError(_) => 8,
        DbErr::IOErr(_) => 9,
        DbErr::UTF8Err(_) => 10,
        DbErr::TypeNotComparable(_, _) => 11,
        DbErr::DataSizeTooLarge(_, _) => 12,
        DbErr::DecodeEOF => 13,
        DbErr::DecodeIntUnknownByte => 14,
        DbErr::DataOverflow => 15,
        DbErr::DataExist(_) => 16,
        DbErr::PageSpaceNotEnough => 17,
        DbErr::DataHasNoPrimaryKey => 18,
        DbErr::ChecksumMismatch => 19,
        DbErr::JournalPageSizeMismatch(_, _) => 20,
        DbErr::SaltMismatch => 21,
        DbErr::PageMagicMismatch(_) => 22,
        DbErr::ItemSizeGreaterThenExpected => 23,
        DbErr::CollectionNotFound(_) => 24,
        DbErr::MetaPageIdError => 25,
        DbErr::CannotWriteDbWithoutTransaction => 26,
        DbErr::StartTransactionInAnotherTransaction => 27,
        DbErr::RollbackNotInTransaction => 28,
        DbErr::Busy => 29

    }
}
