/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::cmp::Ordering;
use std::sync::{Arc, Mutex};
use bson::{Document, Bson};
use crate::btree::BTreePageDelegateWithKey;
use crate::DbResult;
use crate::lsm::LsmKvInner;
use crate::lsm::multi_cursor::MultiCursor;

#[derive(Clone)]
struct CursorItem {
    node:         Arc<Mutex<BTreePageDelegateWithKey>>,
    index:        usize,  // pointer point to the current node
}

impl CursorItem {

    fn new(node: BTreePageDelegateWithKey, index: usize) -> CursorItem {
        CursorItem {
            node: Arc::new(Mutex::new(node)),
            index,
        }
    }

    fn done(&self) -> bool {
        let node_inner = self.node.lock().unwrap();
        self.index >= node_inner.len()
    }

    fn right_pid(&self) -> u32 {
        let node_inner = self.node.lock().unwrap();
        node_inner.right_pid
    }
}

/// Cursor is struct pointing on
/// a value on the kv engine
pub(crate) struct Cursor {
    prefix:    Bson,
    kv_cursor: MultiCursor,
}

impl Cursor {

    pub fn new<T: Into<Bson>>(prefix: T, kv_cursor: MultiCursor) -> Cursor {
        Cursor {
            prefix: prefix.into(),
            kv_cursor,
        }
    }

    pub fn reset(&mut self) {
        self.kv_cursor.reset();
    }

    pub fn reset_by_pkey(&mut self, pkey: &Bson) -> DbResult<bool> {
        let key_buffer = crate::utils::bson::stacked_key([
            &self.prefix,
            pkey,
        ])?;

        self.kv_cursor.seek(&key_buffer)?;

        let key = self.kv_cursor.key();
        if let Some(found) = key {
            return Ok(found.as_ref().cmp(key_buffer.as_slice()) == Ordering::Equal);
        }
        return Ok(false)
    }

    pub fn peek_data(&self, db: &LsmKvInner) -> DbResult<Option<Arc<[u8]>>> {
        self.kv_cursor.value(db)
    }

    pub fn update_current(&mut self, _doc: &Document) -> DbResult<()> {
        unimplemented!()
    }

    #[inline]
    pub fn has_next(&self) -> bool {
        !self.kv_cursor.done()
    }

    pub fn next(&mut self) -> DbResult<()> {
        self.kv_cursor.next()
    }

}
