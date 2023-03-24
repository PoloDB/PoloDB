use std::cmp::Ordering;
use crate::DbResult;
use crate::lsm::lsm_kv::LsmKvInner;
use crate::lsm::lsm_tree::LsmTreeValueMarker;
use crate::lsm::multi_cursor::CursorRepr;

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
