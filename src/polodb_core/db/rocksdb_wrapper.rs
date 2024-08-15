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

use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::{env, ptr};
use std::ffi::CString;
use libc::c_char;
use librocksdb_sys as ffi;
use super::db::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use crate::db::rocksdb_options::RocksDBWaitForCompactOptions;
use crate::db::rocksdb_transaction::RocksDBTransaction;

macro_rules! check_err {
    ($err:expr) => {
        if !$err.is_null() {
            let c_str = std::ffi::CStr::from_ptr($err);

            // Convert the &CStr to a &str
            let str_slice = c_str.to_str().expect("C string is not valid UTF-8");

            // Convert the &str to a String and return
            return Err(crate::Error::RocksDbErr(str_slice.to_owned()))
        }
    };
}

#[derive(Clone)]
pub(crate) struct RocksDBWrapper {
    inner: Arc<Mutex<RocksDBWrapperInner>>,
}

impl RocksDBWrapper {

    pub fn open(path: &Path) -> Result<RocksDBWrapper> {
        let inner = RocksDBWrapperInner::open(path)?;
        Ok(RocksDBWrapper {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub fn begin_transaction(&self) -> Result<RocksDBTransaction> {
        let mut db_inner = self.inner.lock()?;
        RocksDBTransaction::new(db_inner.deref_mut() as *mut _)
    }

}

pub(crate) struct RocksDBWrapperInner {
    #[allow(dead_code)]
    path: String,
    pub(crate) options: *mut ffi::rocksdb_options_t,
    pub(crate) txn_db_options: *mut ffi::rocksdb_transactiondb_options_t,
    pub(crate) inner: *mut ffi::rocksdb_transactiondb_t,
    pub(crate) txn_count: AtomicU64,
}

unsafe impl Send for RocksDBWrapperInner {}
unsafe impl Sync for RocksDBWrapperInner {}

impl RocksDBWrapperInner {

    pub fn open(path: &Path) -> Result<RocksDBWrapperInner> {
        let path: String = path.to_str().unwrap().into();
        unsafe {
            let txn_db_opts = ffi::rocksdb_transactiondb_options_create();
            let options = ffi::rocksdb_options_create();
            ffi::rocksdb_options_set_create_if_missing(options, 1);
            let mut err: *mut c_char = ptr::null_mut();
            let path_c = CString::new(path.clone()).unwrap();
            let db = ffi::rocksdb_transactiondb_open(options, txn_db_opts, path_c.as_ptr(), &mut err);
            check_err!(err);
            Ok(RocksDBWrapperInner {
                path,
                options,
                txn_db_options: txn_db_opts,
                inner: db,
                txn_count: AtomicU64::new(0),
            })
        }
    }

}

impl Drop for RocksDBWrapperInner {
    fn drop(&mut self) {
        unsafe {
            if self.txn_count.load(Ordering::SeqCst) != 0 {
                panic!("there are still transactions opened")
            }
            let mut err: *mut c_char = ptr::null_mut();

            {
                let wait_for_compact_options = RocksDBWaitForCompactOptions::new();
                wait_for_compact_options.set_flush(true);
                ffi::rocksdb_wait_for_compact(self.inner.cast(), wait_for_compact_options.get(), &mut err);
                if !err.is_null() {
                    let c_str = std::ffi::CStr::from_ptr(err);
                    let str_slice = c_str.to_str().expect("C string is not valid UTF-8");
                    eprintln!("wait for compact error: {}", str_slice);
                }
            }

            ffi::rocksdb_transactiondb_flush_wal(self.inner, 1, &mut err);
            if !err.is_null() {
                let c_str = std::ffi::CStr::from_ptr(err);
                let str_slice = c_str.to_str().expect("C string is not valid UTF-8");
                eprintln!("flush wal error: {}", str_slice);
            }

            ffi::rocksdb_transactiondb_close(self.inner);

            ffi::rocksdb_options_destroy(self.options);
            ffi::rocksdb_transactiondb_options_destroy(self.txn_db_options);
        }
    }
}

#[allow(dead_code)]
fn mk_db_path(db_name: &str) -> PathBuf {
    let mut db_path = env::temp_dir();
    let db_filename = String::from(db_name) + "-db";
    db_path.push(db_filename);
    db_path
}

#[test]
fn test_rocks_db() {
    let test_path = mk_db_path("test_rocks_db");

    let _ = std::fs::remove_dir_all(test_path.as_path());

    let db = RocksDBWrapper::open(test_path.as_path()).unwrap();

    let txn = db.begin_transaction().unwrap();
    txn.set(b"key", b"value").unwrap();
    let value = txn.get(b"key").unwrap().unwrap();
    assert_eq!(value, b"value".to_vec());
    txn.commit().unwrap();

    let value = txn.get(b"key").unwrap().unwrap();
    assert_eq!(value, b"value".to_vec());
    txn.set(b"key", b"value2").unwrap();
    assert!(txn.commit().unwrap_err().to_string().contains("committed"));
}

#[test]
#[should_panic]
fn test_close_with_open_txn() {
    let txn = {
        let test_path = mk_db_path("test_close_with_open_txn");

        let _ = std::fs::remove_dir_all(test_path.as_path());

        let db = RocksDBWrapper::open(test_path.as_path()).unwrap();

        let txn = db.begin_transaction().unwrap();
        txn.set(b"key", b"value").unwrap();
        let value = txn.get(b"key").unwrap().unwrap();
        assert_eq!(value, b"value".to_vec());
        txn
    };
    txn.commit().unwrap();
}

#[test]
fn test_open_on_exist_file() {
    use std::io::Write;

    let test_path = mk_db_path("test_open_on_exist_file");

    let _ = std::fs::remove_dir_all(test_path.as_path());

    {
        let mut file = std::fs::File::create(test_path.as_path()).unwrap();
        // write something random
        file.write_all(b"hello world").unwrap();
    }

    let open_err = RocksDBWrapper::open(test_path.as_path());
    assert!(open_err.is_err());
}
