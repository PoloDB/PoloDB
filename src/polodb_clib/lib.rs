/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
#![allow(clippy::missing_safety_doc)]

use polodb_core::{DbErr, Database};
use std::os::raw::{c_char, c_uint, c_int, c_uchar};
use std::ptr::null_mut;
use std::ffi::{c_void, CStr, CString};
use std::mem::size_of;
use std::sync::Arc;
use threadpool::ThreadPool;

macro_rules! try_read_utf8 {
    ($action:expr) => {
        match $action {
            Ok(str) => str,
            Err(err) => {
                return db_error_to_c(err.into());
            }
        }
    }
}

#[repr(C)]
pub struct PoloDbError {
    code:    c_int,
    message: *mut c_char,
}

unsafe fn db_error_to_c(err: DbErr) -> *mut PoloDbError {
    let ptr = libc::malloc(size_of::<PoloDbError>()).cast::<PoloDbError>();

    (*ptr).code = error_code_of_db_err(&err);
    (*ptr).message = {
        let err_msg = err.to_string();
        let str_size = err_msg.len();
        let err_cstring = CString::new(err_msg).unwrap();

        let str_ptr = libc::malloc(str_size + 1).cast::<c_char>();
        libc::memset(str_ptr.cast(), 0, str_size + 1);

        err_cstring.as_ptr().copy_to(str_ptr, str_size);

        str_ptr
    };

    ptr
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_free_error(err: *mut PoloDbError) {
    if err.is_null() {
        return;
    }

    libc::free((*err).message.cast());
    libc::free(err.cast());
}

pub struct DatabaseWrapper {
    db: Arc<Database>,
    thread_pool: ThreadPool,
}

impl DatabaseWrapper {

    fn new(db: Database) -> DatabaseWrapper {
        let thread_pool = ThreadPool::new(1);
        DatabaseWrapper {
            db: Arc::new(db),
            thread_pool
        }
    }

}

#[no_mangle]
pub unsafe extern "C" fn PLDB_open(path: *const c_char, result: *mut *mut DatabaseWrapper) -> *mut PoloDbError {
    let cstr = CStr::from_ptr(path);
    let str = try_read_utf8!(cstr.to_str());
    match Database::open_file(str) {
        Ok(db) => {
            let ptr = Box::new(DatabaseWrapper::new(db));
            let raw_ptr = Box::into_raw(ptr);
            result.write(raw_ptr);
            null_mut()
        },
        Err(err) => {
            db_error_to_c(err)
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_handle_message(
    db_wrapper: *mut DatabaseWrapper,
    msg: *const c_uchar,
    msg_size: u64,
    result: *mut *mut c_uchar,
    result_size: *mut u64
) -> *mut PoloDbError {
    let db_wrapper_ref = db_wrapper.as_ref().unwrap();
    let db_arc = db_wrapper_ref.db.clone();

    let mut req_buf = std::slice::from_raw_parts(msg.cast::<u8>(), msg_size as usize);

    let request_result = db_arc.handle_request::<&[u8]>(&mut req_buf);

    match request_result {
        Ok(request_result) => {
            let bytes = polodb_core::bson::to_vec(&request_result.value).unwrap();
            let ptr = libc::malloc(bytes.len()).cast::<u8>();

            ptr.copy_from_nonoverlapping(bytes.as_ptr(), bytes.len());

            if !result.is_null() {
                result.write(ptr.cast::<c_uchar>());
            }

            if !result_size.is_null() {
                result_size.write(bytes.len() as u64);
            }

            null_mut()
        }
        Err(err) => {
            db_error_to_c(err)
        }
    }
}

struct RawBox<T>(*mut T);

unsafe impl<T> Send for RawBox<T> {}

/// Handle message in another thread,
/// invoke the callback in another thread.
///
/// There is a threadpool running in the background
/// because spawning a thread is expensive.
#[no_mangle]
pub unsafe extern "C" fn PLDB_handle_message_async(
    db_wrapper: *mut DatabaseWrapper,
    msg: *const c_uchar,
    msg_size: u64,
    callback: unsafe extern "C" fn(*mut PoloDbError, *mut c_uchar, u64, *mut c_void),
    raw: *mut c_void,
) {
    let db_wrapper_ref = db_wrapper.as_ref().unwrap();
    let db_arc = db_wrapper_ref.db.clone();

    let raw_data_wrapper = RawBox(raw);

    // copy message to a buffer
    let mut msg_buffer: Vec<c_uchar> = Vec::new();
    msg_buffer.resize(msg_size as usize, 0);
    msg.copy_to_nonoverlapping(msg_buffer.as_mut_ptr(), msg_size as usize);

    db_wrapper_ref.thread_pool.execute(move || {
        let mut msg_slice = msg_buffer.as_slice();

        let request_result = db_arc.handle_request::<&[u8]>(&mut msg_slice);
        match request_result {
            Ok(request_result) => {
                let bytes = polodb_core::bson::to_vec(&request_result.value).unwrap();
                let ptr = libc::malloc(bytes.len()).cast::<u8>();
                ptr.copy_from_nonoverlapping(bytes.as_ptr(), bytes.len());
                callback(null_mut(), ptr, bytes.len() as u64, raw_data_wrapper.0);
            }
            Err(err) => {
                let c_error = db_error_to_c(err);
                callback(c_error, null_mut(), 0, raw_data_wrapper.0);
            }
        }
    });
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_free_result(msg: *mut c_char) {
    libc::free(msg.cast());
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_version(buffer: *mut c_char, buffer_size: c_uint) -> c_uint {
    let version_str = Database::get_version();
    let str_size = version_str.len();
    let cstring = CString::new(version_str).unwrap();
    let c_ptr = cstring.as_ptr();
    let expected_size: usize = std::cmp::min(str_size, buffer_size as usize);
    c_ptr.copy_to(buffer, expected_size);
    expected_size as c_uint
}

#[no_mangle]
pub unsafe extern "C" fn PLDB_close(db: *mut DatabaseWrapper) {
    let db_wrapper = Box::from_raw(db);
    db_wrapper.thread_pool.join();
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
        // DbErr::CollectionIdNotFound(_) => 25,
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
        // DbErr::MetaVersionMismatched(_, _) => 40,
        DbErr::Busy => 41,
        DbErr::InvalidField(_) => 42,
        DbErr::CollectionAlreadyExits(_) => 43,
        DbErr::UnableToUpdatePrimaryKey => 44,
        DbErr::UnexpectedTypeForOp(_) => 45,
        DbErr::NotAValidDatabase => 46,
        DbErr::DatabaseOccupied => 47,
        DbErr::Multiple(_) => 48,
        DbErr::VersionMismatch(_) => 49,
        DbErr::BsonDeErr(_) => 51,
        DbErr::LockError => 52,
        DbErr::CannotApplyOperation(_) => 53,
        DbErr::NoTransactionStarted => 54,
        DbErr::InvalidSession(_) => 55,
        DbErr::SessionOutdated => 56,
        DbErr::DbIsClosed => 57,
        DbErr::FromUtf8Error(_) => 58,
    }
}
