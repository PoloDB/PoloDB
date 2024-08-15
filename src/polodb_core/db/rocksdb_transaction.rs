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
use std::ptr;
use std::ptr::null_mut;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use libc::c_char;
use polodb_librocksdb_sys as ffi;
use crate::db::rocksdb_options::{RocksDBReadOptions, RocksDBTransactionOptions, RocksDBWriteOptions};
use crate::db::rocksdb_wrapper::RocksDBWrapperInner;
use crate::db::RocksDBIterator;
use super::db::Result;

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
pub(crate) struct RocksDBTransaction {
    inner: Arc<Mutex<RocksDBTransactionInner>>,
}

impl RocksDBTransaction {

    pub(crate) fn new(db_inner: *mut RocksDBWrapperInner) -> Result<RocksDBTransaction>  {
        let inner = RocksDBTransactionInner::new(db_inner)?;
        Ok(RocksDBTransaction {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub fn new_iterator(&self) -> RocksDBIterator {
        let mut inner = self.inner.lock().unwrap();
        RocksDBIterator::new(inner.deref_mut() as *mut RocksDBTransactionInner)
    }

    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        inner.set(key, value)
    }

    #[allow(dead_code)]
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.lock().unwrap();
        inner.get(key)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        inner.delete(key)
    }

    pub fn rollback(&self) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        inner.rollback()
    }

    pub fn commit(&self) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        inner.commit()
    }

}

pub(crate) struct RocksDBTransactionInner {
    pub(crate) read_options: RocksDBReadOptions,
    _write_options: RocksDBWriteOptions,
    _txn_options: RocksDBTransactionOptions,
    pub(crate) inner: *mut ffi::rocksdb_transaction_t,
    db_inner: *mut RocksDBWrapperInner,
    pub(crate) iter_count: AtomicU64,
}

unsafe impl Send for RocksDBTransactionInner {}
unsafe impl Sync for RocksDBTransactionInner {}

impl RocksDBTransactionInner {

    pub(crate) fn new(db_inner: *mut RocksDBWrapperInner) -> Result<RocksDBTransactionInner>  {
        unsafe {
            let read_options = RocksDBReadOptions::new();
            let write_options = RocksDBWriteOptions::new();
            write_options.set_sync(true);
            let txn_options = RocksDBTransactionOptions::new();
            _ = (*db_inner).txn_count.fetch_add(1, Ordering::SeqCst);
            let inner = ffi::rocksdb_transaction_begin(
                (*db_inner).inner,
                write_options.get(),
                txn_options.get(),
                null_mut(),
            );

            Ok(RocksDBTransactionInner {
                read_options,
                _write_options: write_options,
                _txn_options: txn_options,
                inner,
                db_inner,
                iter_count: AtomicU64::new(0),
            })
        }
    }

    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        unsafe {
            let mut err: *mut c_char = ptr::null_mut();

            ffi::rocksdb_transaction_put(
                self.inner,
                key.as_ptr() as *const i8,
                key.len(),
                value.as_ptr() as *const i8,
                value.len(),
                &mut err,
            );

            check_err!(err);
            Ok(())
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        unsafe {
            let mut err: *mut c_char = ptr::null_mut();
            let mut value_len: usize = 0;
            let value = ffi::rocksdb_transaction_get(
                self.inner,
                self.read_options.get(),
                key.as_ptr() as *const i8,
                key.len(),
                &mut value_len,
                &mut err,
            );

            check_err!(err);

            if value.is_null() {
                return Ok(None);
            }

            let value = std::slice::from_raw_parts(value as *const u8, value_len).to_vec();
            Ok(Some(value))
        }
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        unsafe {
            let mut err: *mut c_char = ptr::null_mut();

            ffi::rocksdb_transaction_delete(
                self.inner,
                key.as_ptr() as *const i8,
                key.len(),
                &mut err,
            );

            check_err!(err);
            Ok(())
        }
    }

    pub fn rollback(&self) -> Result<()> {
        unsafe {
            let mut err: *mut c_char = ptr::null_mut();
            ffi::rocksdb_transaction_rollback(self.inner, &mut err);

            check_err!(err);
            Ok(())
        }
    }

    pub(crate) fn commit(&self) -> Result<()> {
        unsafe {
            let mut err: *mut c_char = ptr::null_mut();

            ffi::rocksdb_transaction_commit(self.inner, &mut err);

            check_err!(err);
            Ok(())
        }
    }

}

impl Drop for RocksDBTransactionInner {

    fn drop(&mut self) {
        unsafe {
            if self.iter_count.load(Ordering::SeqCst) != 0 {
                panic!("there are still iterators opened")
            }
            ffi::rocksdb_transaction_destroy(self.inner);
            _ = self.db_inner.as_mut().unwrap().txn_count.fetch_sub(1, Ordering::SeqCst)
        }
    }
}
