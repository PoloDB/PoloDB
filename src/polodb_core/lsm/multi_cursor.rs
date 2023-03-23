/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::cmp::Ordering;
use crate::DbResult;
use crate::lsm::lsm_kv::LsmKvInner;
use crate::lsm::lsm_segment::LsmTuplePtr;
use crate::lsm::lsm_tree::LsmTreeValueMarker;
use super::lsm_tree::TreeCursor;

pub(crate)  enum CursorRepr {
    MemTableCursor(TreeCursor<Box<[u8]>, Vec<u8>>),
    SegTableCursor(TreeCursor<Box<[u8]>, LsmTuplePtr>),
}

impl CursorRepr {

    pub fn seek(&mut self, key: &[u8]) -> Option<Ordering> {
        match self {
            CursorRepr::MemTableCursor(cursor) => {
                cursor.seek(key)
            }
            CursorRepr::SegTableCursor(cursor) => {
                cursor.seek(key)
            }
        }
    }

    pub fn value(&self, db: &LsmKvInner) -> DbResult<Option<LsmTreeValueMarker<Vec<u8>>>> {
        match self {
            CursorRepr::MemTableCursor(mem_table_cursor) => {
                let result = mem_table_cursor.value();
                Ok(result)
            }
            CursorRepr::SegTableCursor(cursor) => {
                let ptr = cursor.value();
                if ptr.is_none() {
                    return Ok(None);
                }
                let marker = ptr.unwrap();
                let result = match marker {
                    LsmTreeValueMarker::Deleted => LsmTreeValueMarker::Deleted,
                    LsmTreeValueMarker::DeleteStart => LsmTreeValueMarker::DeleteStart,
                    LsmTreeValueMarker::DeleteEnd => LsmTreeValueMarker::DeleteEnd,
                    LsmTreeValueMarker::Value(tuple) => {
                        let buffer = db.read_segment_by_ptr(tuple)?;
                        LsmTreeValueMarker::Value(buffer)
                    }
                };
                Ok(Some(result))
            }
        }
    }

    pub fn marker(&self) -> DbResult<Option<LsmTreeValueMarker<()>>> {
        match self {
            CursorRepr::MemTableCursor(mem_table_cursor) => {
                let result = mem_table_cursor.marker();
                Ok(result)
            }
            CursorRepr::SegTableCursor(cursor) => {
                let result = cursor.marker();
                Ok(result)
            }
        }
    }

    pub fn next(&mut self) -> DbResult<()> {
        match self {
            CursorRepr::MemTableCursor(mem_table_cursor) => {
                mem_table_cursor.next();
                Ok(())
            }
            CursorRepr::SegTableCursor(cursor) => {
                cursor.next();
                Ok(())
            }
        }
    }

}

impl Into<CursorRepr> for TreeCursor<Box<[u8]>, Vec<u8>> {

    fn into(self) -> CursorRepr {
        CursorRepr::MemTableCursor(self)
    }

}

impl Into<CursorRepr> for TreeCursor<Box<[u8]>, LsmTuplePtr> {

    fn into(self) -> CursorRepr {
        CursorRepr::SegTableCursor(self)
    }

}

/// This is a cursor used to iterate
/// kv on multi-level lsm-tree.
pub(crate) struct MultiCursor {
    cursors: Vec<CursorRepr>,
    seeks: Vec<Option<Ordering>>,
    first_result: i64,
}

impl MultiCursor {

    pub fn new(cursors: Vec<CursorRepr>) -> MultiCursor {
        let len = cursors.len();
        MultiCursor {
            cursors,
            seeks: vec![None; len],
            first_result: -1,
        }
    }

    pub fn seek(&mut self, key: &[u8]) -> DbResult<()> {
        self.first_result = -1;
        let mut idx: usize = 0;
        let mut done = false;

        for cursor in &mut self.cursors {
            let tmp = cursor.seek(key);
            self.seeks[idx] = tmp;

            if tmp.is_some() && !done {
                self.first_result = idx as i64;
                done = true;
            }

            idx += 1;
        }
        Ok(())
    }

    pub fn value(&self, db: &LsmKvInner) -> DbResult<Option<Vec<u8>>> {
        if self.first_result >= 0 {
            let cursor = &self.cursors[self.first_result as usize];
            let tmp = cursor.value(db)?;
            if tmp.is_none() {
                return Ok(None);
            }
            let result = tmp.unwrap().into();
            return Ok(result);
        }

        Ok(None)
    }

    pub fn next(&mut self) -> DbResult<()> {
        loop {
            let top = self.cursors.first_mut().unwrap();
            top.next()?;

            let val = top.marker()?;
            match val {
                None => {
                    return Ok(());
                },
                Some(LsmTreeValueMarker::Value(_)) => {
                    return Ok(());
                }
                _ => ()
            }
        }
    }

}
