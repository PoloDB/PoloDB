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

use std::cmp::Ordering;
use std::sync::Arc;
use bson::Bson;
use crate::db::RocksDBIterator;
use crate::Result;
use crate::transaction::TransactionInner;

/// Cursor is struct pointing on
/// a value on the kv engine
pub(crate) struct Cursor {
    pub(crate)  prefix_bytes: Vec<u8>,
    kv_cursor:    RocksDBIterator,
    current_key:  Option<Arc<[u8]>>,
}

impl Cursor {

    pub fn new_with_str_prefix<T: Into<String>>(s: T, kv_cursor: RocksDBIterator) -> Result<Cursor> {
        let mut prefix_bytes = Vec::<u8>::new();
        crate::utils::bson::stacked_key_bytes(&mut prefix_bytes, &Bson::String(s.into()))?;
        let cursor = Cursor::new(prefix_bytes, kv_cursor);
        Ok(cursor)
    }

    pub fn new(prefix_bytes: Vec<u8>, kv_cursor: RocksDBIterator) -> Cursor {
        Cursor {
            prefix_bytes,
            kv_cursor,
            current_key: None,
        }
    }

    #[inline]
    pub fn copy_data(&self) -> Result<Vec<u8>> {
        self.kv_cursor.copy_data()
    }

    pub fn update_current(&mut self, txn: &TransactionInner, value: &[u8]) -> Result<bool> {
        if let Some(key) = &self.current_key {
            txn.rocksdb_txn.set(key.as_ref(), value)?;
            return Ok(true);
        }
        Ok(false)
    }


    pub fn reset(&mut self) -> Result<()> {
        self.kv_cursor.seek(self.prefix_bytes.as_slice());

        if self.kv_cursor.valid() {
            self.current_key = Some(self.kv_cursor.copy_key_arc()?);
        }

        Ok(())
    }

    pub fn reset_by_pkey(&mut self, pkey: &Bson) -> Result<bool> {
        let mut key_buffer = self.prefix_bytes.clone();

        {
            let primary_key_buffer = crate::utils::bson::stacked_key([
                pkey,
            ])?;

            key_buffer.extend_from_slice(&primary_key_buffer);
        }

        self.reset_by_custom_key(key_buffer.as_slice())
    }

    #[allow(dead_code)]
    pub fn reset_by_pkey_buf(&mut self, pkey_buffer: &[u8]) -> Result<bool> {
        let mut key_buffer = self.prefix_bytes.clone();

        key_buffer.extend_from_slice(pkey_buffer);

        self.reset_by_custom_key(key_buffer.as_slice())
    }

    fn reset_by_custom_key(&mut self, key_buffer: &[u8]) -> Result<bool> {
        self.kv_cursor.seek(key_buffer);

        if self.kv_cursor.valid() {
            self.current_key = Some(self.kv_cursor.copy_key_arc()?);
            if let Some(found) = &self.current_key {
                return Ok(found.as_ref().cmp(key_buffer) == Ordering::Equal);
            }
        }

        Ok(false)
    }

    pub fn reset_by_index_value(&mut self, index_value: &Bson) -> Result<bool> {
        let key_buffer = {
            let mut key_buffer = self.prefix_bytes.clone();
            let primary_key_buffer = crate::utils::bson::stacked_key([
                index_value,
            ])?;

            key_buffer.extend_from_slice(&primary_key_buffer);

            key_buffer
        };

        self.kv_cursor.seek(key_buffer.as_slice());

        if self.kv_cursor.valid() {
            self.current_key = Some(self.kv_cursor.copy_key_arc()?);
            if let Some(found) = &self.current_key {
                let starts_with = found.as_ref().starts_with(key_buffer.as_slice());
                return Ok(starts_with);
            }
        }

        Ok(false)
    }

    pub fn peek_key(&self) -> Option<Arc<[u8]>> {
        self.current_key.clone()
    }

    pub fn has_next(&self) -> bool {
        if !self.kv_cursor.valid() {
            return false;
        }

        if let Some(current_key) = &self.current_key {
            if !current_key.starts_with(self.prefix_bytes.as_slice()) {
                return false;
            }
            true
        } else {
            false
        }
    }

    pub fn next(&mut self) -> Result<()> {
        self.kv_cursor.next();
        if !self.kv_cursor.valid() {
            self.current_key = None;
            return Ok(());
        }
        self.current_key = Some(self.kv_cursor.copy_key_arc()?);
        Ok(())
    }

}
