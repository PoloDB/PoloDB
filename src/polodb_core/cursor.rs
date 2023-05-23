/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::cmp::Ordering;
use std::sync::Arc;
use bson::Bson;
use crate::Result;
use crate::lsm::LsmKvInner;
use crate::lsm::multi_cursor::MultiCursor;
use crate::session::SessionInner;

/// Cursor is struct pointing on
/// a value on the kv engine
pub(crate) struct Cursor {
    prefix_bytes: Vec<u8>,
    kv_cursor:    MultiCursor,
    current_key:  Option<Arc<[u8]>>,
}

impl Cursor {

    pub fn new_with_str_prefix<T: Into<String>>(s: T, kv_cursor: MultiCursor) -> Result<Cursor> {
        let mut prefix_bytes = Vec::<u8>::new();
        crate::utils::bson::stacked_key_bytes(&mut prefix_bytes, &Bson::String(s.into()))?;
        let cursor = Cursor::new(prefix_bytes, kv_cursor);
        Ok(cursor)
    }

    pub fn new(prefix_bytes: Vec<u8>, kv_cursor: MultiCursor) -> Cursor {
        Cursor {
            prefix_bytes,
            kv_cursor,
            current_key: None,
        }
    }

    pub fn update_current(&mut self, session: &mut SessionInner, value: &[u8]) -> Result<bool> {
        session.kv_session_mut().update_cursor_current(&mut self.kv_cursor, value)
    }

    #[inline]
    pub fn multi_cursor_mut(&mut self) -> &mut MultiCursor {
        &mut self.kv_cursor
    }

    pub fn reset(&mut self) -> Result<()> {
        self.kv_cursor.seek(self.prefix_bytes.as_slice())?;

        self.current_key = self.kv_cursor.key();

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

    pub fn reset_by_pkey_buf(&mut self, pkey_buffer: &[u8]) -> Result<bool> {
        let mut key_buffer = self.prefix_bytes.clone();

        key_buffer.extend_from_slice(pkey_buffer);

        self.reset_by_custom_key(key_buffer.as_slice())
    }

    fn reset_by_custom_key(&mut self, key_buffer: &[u8]) -> Result<bool> {
        self.kv_cursor.seek(key_buffer)?;

        self.current_key = self.kv_cursor.key();
        if let Some(found) = &self.current_key {
            return Ok(found.as_ref().cmp(key_buffer) == Ordering::Equal);
        }
        return Ok(false)
    }

    #[allow(dead_code)]
    pub fn peek_key(&self) -> Option<Arc<[u8]>> {
        self.current_key.clone()
    }

    pub fn peek_data(&self, db: &LsmKvInner) -> Result<Option<Arc<[u8]>>> {
        if let Some(current_key) = &self.current_key {
            if !is_prefix_with(&current_key, &self.prefix_bytes) {
                return Ok(None);
            }

            self.kv_cursor.value(db)
        } else {
            Ok(None)
        }
    }

    pub fn has_next(&self) -> bool {
        if self.kv_cursor.done() {
            return false;
        }

        if let Some(current_key) = &self.current_key {
            if !is_prefix_with(&current_key, &self.prefix_bytes) {
                return false;
            }
            true
        } else {
            false
        }
    }

    pub fn next(&mut self) -> Result<()> {
        self.kv_cursor.next()?;
        self.current_key = self.kv_cursor.key();
        Ok(())
    }

}

#[inline]
fn is_prefix_with(target: &[u8], prefix: &[u8]) -> bool {
    if target.len() < prefix.len() {
        return false;
    }

    target[0..prefix.len()].cmp(prefix) == Ordering::Equal
}
