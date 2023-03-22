/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::cmp::Ordering;
use crate::DbResult;
use crate::lsm::lsm_tree::LsmTreeValueMarker;
use super::lsm_tree::TreeCursor;

enum CursorRepr {
    MemTableCursor(TreeCursor<Box<[u8]>, Vec<u8>>),
}

impl CursorRepr {

    pub fn seek(&mut self, key: &[u8]) -> DbResult<Ordering> {
        match self {
            CursorRepr::MemTableCursor(mem_table_cursor) => {
                let ord = mem_table_cursor.seek(key);
                Ok(ord)
            }
        }
    }

    pub fn value(&self) -> DbResult<Option<LsmTreeValueMarker<Vec<u8>>>> {
        match self {
            CursorRepr::MemTableCursor(mem_table_cursor) => {
                let result = mem_table_cursor.value();
                Ok(result)
            }
        }
    }

    pub fn marker(&self) -> DbResult<Option<LsmTreeValueMarker<()>>> {
        match self {
            CursorRepr::MemTableCursor(mem_table_cursor) => {
                let result = mem_table_cursor.marker();
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
        }
    }

}

impl Into<CursorRepr> for TreeCursor<Box<[u8]>, Vec<u8>> {

    fn into(self) -> CursorRepr {
        CursorRepr::MemTableCursor(self)
    }

}

/// This is a cursor used to iterate
/// kv on multi-level lsm-tree.
pub(crate) struct MultiCursor {
    cursors: Vec<CursorRepr>,
}

impl MultiCursor {

    pub fn new(mem_table_cursor: TreeCursor<Box<[u8]>, Vec<u8>>) -> MultiCursor {
        MultiCursor {
            cursors: vec![mem_table_cursor.into()],
        }
    }

    pub fn seek(&mut self, key: &[u8]) -> DbResult<()> {
        for cursor in &mut self.cursors {
            cursor.seek(key)?;
        }
        Ok(())
    }

    pub fn value(&self) -> DbResult<Option<Vec<u8>>> {
        let top = self.cursors.first().unwrap();
        let val = top.value()?;
        let buffer = val.map(| marker | { marker.unwrap() });
        Ok(buffer)
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
