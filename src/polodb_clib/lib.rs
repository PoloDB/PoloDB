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
use polodb_core::{Database, DbErr, ByteCodeBuilder};
use polodb_core::bson::{Value, ObjectId, Document};

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
pub extern "C" fn PLDB_new_bytecode_builder() -> *mut ByteCodeBuilder {
    let builder = Box::new(ByteCodeBuilder::new());
    Box::into_raw(builder)
}

#[no_mangle]
pub extern "C" fn PLDB_bcb_add_static_val(builder: *mut ByteCodeBuilder, val: *mut Value) -> c_uint {
    unsafe {
        let copy = val.as_ref().unwrap().clone();

        builder.as_mut().unwrap().add_static_values(copy) as c_uint
    }
}

#[no_mangle]
pub extern "C" fn PLDB_bcb_add_divider(builder: *mut ByteCodeBuilder) {
    unsafe {
        builder.as_mut().unwrap().add_divider();
    }
}

#[no_mangle]
pub extern "C" fn PLDB_free_byte_code_builder(builder: *mut ByteCodeBuilder) {
    let _ptr = unsafe { Box::from_raw(builder) };
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
pub extern "C" fn PLDB_mk_str(str: *mut c_char) -> *mut Value {
    let str = unsafe { CString::from_raw(str) };
    let rust_str = try_read_utf8!(str.to_str(), null_mut());
    let val = Box::new(Value::String(Rc::new(rust_str.to_string())));
    Box::into_raw(val)
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
pub extern "C" fn PLDB_mk_object_id(bytes: *mut c_uchar) -> *mut Value {
    let mut data: [u8; 12] = [0; 12];
    unsafe {
        bytes.copy_to(data.as_mut_ptr(), 12);
    }
    let oid = match ObjectId::deserialize(&data) {
        Ok(oid) => oid,
        Err(err) => {
            eprintln!("parse object oid error: {}", err);
            return null_mut();
        }
    };
    let val = Box::new(Value::ObjectId(Rc::new(oid)));
    Box::into_raw(val)
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
