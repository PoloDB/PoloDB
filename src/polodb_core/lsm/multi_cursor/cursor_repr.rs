use std::sync::Arc;
use std::cmp::Ordering;
use crate::DbResult;
use crate::lsm::lsm_kv::LsmKvInner;
use crate::lsm::lsm_segment::LsmTuplePtr;
use crate::lsm::lsm_tree::{LsmTree, LsmTreeValueMarker, TreeCursor};
use crate::lsm::mem_table::MemTable;

pub(crate) enum CursorRepr {
    MemTableCursor(TreeCursor<Arc<[u8]>, Arc<[u8]>>),
    SegTableCursor(TreeCursor<Arc<[u8]>, LsmTuplePtr>),
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

    pub fn update_current(
        &mut self,
        value: &LsmTreeValueMarker<Arc<[u8]>>,
    ) -> Option<(LsmTree<Arc<[u8]>, Arc<[u8]>>, Option<Arc<[u8]>>)> {
        match self {
            CursorRepr::MemTableCursor(cursor) => {
                cursor.update(value)
            }
            _ => unreachable!(),
        }
    }

    pub fn go_to_min(&mut self) -> DbResult<()> {
        match self {
            CursorRepr::MemTableCursor(cursor) => {
                cursor.go_to_min();
                Ok(())
            }
            CursorRepr::SegTableCursor(cursor) => {
                cursor.go_to_min();
                Ok(())
            }
        }
    }

    pub fn key(&self) -> Option<Arc<[u8]>> {
        match self {
            CursorRepr::MemTableCursor(cursor) => cursor.key(),
            CursorRepr::SegTableCursor(cursor) => cursor.key(),
        }
    }

    pub fn value(&self, db: &LsmKvInner) -> DbResult<Option<LsmTreeValueMarker<Arc<[u8]>>>> {
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

    pub fn reset(&mut self) {
        match self {
            CursorRepr::MemTableCursor(cursor) => cursor.reset(),
            CursorRepr::SegTableCursor(cursor) => cursor.reset(),
        }
    }

    pub fn done(&self) -> bool {
        match self {
            CursorRepr::MemTableCursor(cursor) => cursor.done(),
            CursorRepr::SegTableCursor(cursor) => cursor.done(),
        }
    }

    pub fn unwrap_tuple_ptr(&self) -> LsmTreeValueMarker<LsmTuplePtr> {
        match self {
            CursorRepr::SegTableCursor(cursor) => cursor.value().unwrap(),
            _ => panic!("this is not seg table"),
        }
    }

}

impl Into<CursorRepr> for TreeCursor<Arc<[u8]>, Arc<[u8]>> {

    fn into(self) -> CursorRepr {
        CursorRepr::MemTableCursor(self)
    }

}

impl Into<CursorRepr> for TreeCursor<Arc<[u8]>, LsmTuplePtr> {

    fn into(self) -> CursorRepr {
        CursorRepr::SegTableCursor(self)
    }

}
