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

use crate::db::RocksDBTransaction;

#[derive(Clone)]
pub(crate) struct TransactionInner {
    pub(crate) rocksdb_txn: RocksDBTransaction,
    auto_commit: bool,
}

impl TransactionInner {

    pub fn new(rocksdb_txn: RocksDBTransaction) -> TransactionInner {
        TransactionInner {
            rocksdb_txn,
            auto_commit: true,
        }
    }

    pub fn set_auto_commit(&mut self, auto_commit: bool) {
        self.auto_commit = auto_commit;
    }

    #[inline]
    #[allow(dead_code)]
    pub fn is_auto_commit(&self) -> bool {
        self.auto_commit
    }

    #[inline]
    pub fn put(&self, key: &[u8], value: &[u8]) -> crate::Result<()> {
        self.rocksdb_txn.set(key, value)
    }

    #[inline]
    pub fn delete(&self, key: &[u8]) -> crate::Result<()> {
        self.rocksdb_txn.delete(key)
    }

    #[inline]
    pub fn commit(&self) -> crate::Result<()> {
        self.rocksdb_txn.commit()
    }

    pub(crate) fn auto_commit(&self) -> crate::Result<()> {
        if self.auto_commit {
            self.rocksdb_txn.commit()
        } else {
            Ok(())
        }
    }

    #[inline]
    pub fn rollback(&self) -> crate::Result<()> {
        self.rocksdb_txn.rollback()
    }

}
