use std::cmp::Ordering;
use crate::DbResult;
use super::lsm_tree::TreeCursor;

enum CursorRepr {
    MemTableCursor(TreeCursor<Box<[u8]>, Box<[u8]>>),
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

}

impl Into<CursorRepr> for TreeCursor<Box<[u8]>, Box<[u8]>> {

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

    pub fn new(mem_table_cursor: TreeCursor<Box<[u8]>, Box<[u8]>>) -> MultiCursor {
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

}
