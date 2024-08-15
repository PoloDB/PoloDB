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

use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use libc::c_char;
use crate::db::rocksdb_transaction::RocksDBTransactionInner;
use polodb_librocksdb_sys as ffi;
use super::db::Result;

#[derive(Clone)]
pub(crate) struct RocksDBIterator {
    inner: Arc<RocksDBIteratorInner>
}

impl RocksDBIterator {
    pub(crate) fn new(txn_inner: *mut RocksDBTransactionInner) -> RocksDBIterator {
        let inner = RocksDBIteratorInner::new(txn_inner);
        RocksDBIterator {
            inner: Arc::new(inner),
        }
    }

    pub fn seek_to_first(&self) {
        self.inner.seek_to_first()
    }

    pub fn seek(&self, key: &[u8]) {
        self.inner.seek(key)
    }

    pub fn valid(&self) -> bool {
        self.inner.valid()
    }

    pub fn next(&self) {
        self.inner.next()
    }

    #[allow(dead_code)]
    pub fn prev(&self) {
        self.inner.prev()
    }

    #[allow(dead_code)]
    pub fn error(&self) -> Result<()> {
        self.inner.error()
    }

    pub fn copy_key(&self) -> Result<Vec<u8>> {
        self.inner.copy_key()
    }

    pub fn copy_key_arc(&self) -> Result<Arc<[u8]>> {
        self.inner.copy_key_arc()
    }

    pub fn copy_data(&self) -> Result<Vec<u8>> {
        self.inner.copy_data()
    }
}

pub(crate) struct RocksDBIteratorInner {
    inner: *mut ffi::rocksdb_iterator_t,
    txn_inner: *mut RocksDBTransactionInner,
}

unsafe impl Send for RocksDBIteratorInner {}
unsafe impl Sync for RocksDBIteratorInner {}

impl RocksDBIteratorInner {

    pub(crate) fn new(txn_inner: *mut RocksDBTransactionInner) -> RocksDBIteratorInner {
        unsafe {
            let txn_ptr = (*txn_inner).inner;
            let read_options = &(*txn_inner).read_options;
            let iter = ffi::rocksdb_transaction_create_iterator(txn_ptr, read_options.get());
            _ = (*txn_inner).iter_count.fetch_add(1, Ordering::SeqCst);
            RocksDBIteratorInner {
                inner: iter,
                txn_inner,
            }
        }
    }

    pub fn seek_to_first(&self) {
        unsafe {
            ffi::rocksdb_iter_seek_to_first(self.inner);
        }
    }

    pub fn seek(&self, key: &[u8]) {
        unsafe {
            ffi::rocksdb_iter_seek(self.inner, key.as_ptr() as *const i8, key.len());
        }
    }

    pub fn valid(&self) -> bool {
        unsafe {
            ffi::rocksdb_iter_valid(self.inner) != 0
        }
    }

    pub fn next(&self) {
        unsafe {
            ffi::rocksdb_iter_next(self.inner);
        }
    }

    pub fn prev(&self) {
        unsafe {
            ffi::rocksdb_iter_prev(self.inner);
        }
    }

    pub fn error(&self) -> Result<()> {
        unsafe {
            let mut err: *mut c_char = null_mut();
            ffi::rocksdb_iter_get_error(self.inner, &mut err);
            if !err.is_null() {
                let c_str = std::ffi::CStr::from_ptr(err);

                // Convert the &CStr to a &str
                let str_slice = c_str.to_str().expect("C string is not valid UTF-8");

                // Convert the &str to a String and return
                return Err(crate::Error::RocksDbErr(str_slice.to_owned()))
            }
            Ok(())
        }
    }

    pub fn copy_key(&self) -> Result<Vec<u8>> {
        self.error()?;
        unsafe {
            let mut len: usize = 0;
            let key = ffi::rocksdb_iter_key(self.inner, &mut len);
            let key = std::slice::from_raw_parts(key as *const u8, len);
            Ok(key.to_vec())
        }
    }

    pub fn copy_key_arc(&self) -> Result<Arc<[u8]>> {
        self.error()?;
        unsafe {
            let mut len: usize = 0;
            let key = ffi::rocksdb_iter_key(self.inner, &mut len);
            let key = std::slice::from_raw_parts(key as *const u8, len);
            Ok(Arc::from(key))
        }
    }

    pub fn copy_data(&self) -> Result<Vec<u8>> {
        self.error()?;
        unsafe {
            let mut len: usize = 0;
            let data = ffi::rocksdb_iter_value(self.inner, &mut len);
            let data = std::slice::from_raw_parts(data as *const u8, len);
            Ok(data.to_vec())
        }
    }

}

impl Drop for RocksDBIteratorInner {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_iter_destroy(self.inner);
            _ = (*self.txn_inner).iter_count.fetch_sub(1, Ordering::SeqCst);
        }
    }
}
