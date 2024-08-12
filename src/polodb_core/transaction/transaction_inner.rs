/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
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
