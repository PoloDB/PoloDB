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

use librocksdb_sys as ffi;

pub(crate) struct RocksDBWaitForCompactOptions {
    inner: *mut ffi::rocksdb_wait_for_compact_options_t,
}

impl RocksDBWaitForCompactOptions {

    pub(crate) fn new() -> RocksDBWaitForCompactOptions {
        let inner = unsafe { ffi::rocksdb_wait_for_compact_options_create() };
        assert_eq!(inner.is_null(), false, "rocksdb_wait_for_compact_options_create failed");
        RocksDBWaitForCompactOptions { inner }
    }

    pub(crate) fn get(&self) -> *mut ffi::rocksdb_wait_for_compact_options_t {
        self.inner
    }

    pub(crate) fn set_flush(&self, flush: bool) {
        unsafe {
            ffi::rocksdb_wait_for_compact_options_set_flush(self.inner, if flush {
                1
            } else {
                0
            })
        }
    }

}

impl Drop for RocksDBWaitForCompactOptions {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_wait_for_compact_options_destroy(self.inner) }
    }
}

pub(crate) struct RocksDBWriteOptions {
    inner: *mut ffi::rocksdb_writeoptions_t,
}

impl RocksDBWriteOptions {

    pub(crate) fn new() -> RocksDBWriteOptions {
        let inner = unsafe { ffi::rocksdb_writeoptions_create() };
        assert_eq!(inner.is_null(), false, "rocksdb_writeoptions_create failed");
        RocksDBWriteOptions { inner }
    }

    pub(crate) fn get(&self) -> *mut ffi::rocksdb_writeoptions_t {
        self.inner
    }

    pub(crate) fn set_sync(&self, sync: bool) {
        unsafe {
            ffi::rocksdb_writeoptions_set_sync(self.inner, if sync {
                1
            } else {
                0
            })
        }
    }

}

impl Drop for RocksDBWriteOptions {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_writeoptions_destroy(self.inner) }
    }
}

pub(crate) struct RocksDBReadOptions {
    inner: *mut ffi::rocksdb_readoptions_t,
}

impl RocksDBReadOptions {

    pub(crate) fn new() -> RocksDBReadOptions {
        let inner = unsafe { ffi::rocksdb_readoptions_create() };
        assert_eq!(inner.is_null(), false, "rocksdb_readoptions_create failed");
        RocksDBReadOptions { inner }
    }

    pub(crate) fn get(&self) -> *mut ffi::rocksdb_readoptions_t {
        self.inner
    }

}

impl Drop for RocksDBReadOptions {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_readoptions_destroy(self.inner) }
    }
}

pub(crate) struct RocksDBTransactionOptions {
    inner: *mut ffi::rocksdb_transaction_options_t,
}

impl RocksDBTransactionOptions {

    pub(crate) fn new() -> RocksDBTransactionOptions {
        let inner = unsafe { ffi::rocksdb_transaction_options_create() };
        assert_eq!(inner.is_null(), false, "rocksdb_transaction_options_create failed");
        RocksDBTransactionOptions { inner }
    }

    pub(crate) fn get(&self) -> *mut ffi::rocksdb_transaction_options_t {
        self.inner
    }

}

impl Drop for RocksDBTransactionOptions {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_transaction_options_destroy(self.inner) }
    }
}

