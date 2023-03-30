use std::cmp::Ordering;
use std::sync::Arc;
use crate::DbResult;
use crate::lsm::lsm_kv::LsmKvInner;
use crate::lsm::lsm_segment::LsmTuplePtr;
use crate::lsm::lsm_tree::LsmTreeValueMarker;
use crate::lsm::multi_cursor::CursorRepr;

/// This is a cursor used to iterate
/// kv on multi-level lsm-tree.
pub(crate) struct MultiCursor {
    cursors: Vec<CursorRepr>,
    keys: Vec<Option<Arc<[u8]>>>,
    first_result: i64,
}

impl MultiCursor {

    pub fn new(cursors: Vec<CursorRepr>) -> MultiCursor {
        let len = cursors.len();
        MultiCursor {
            cursors,
            keys: vec![None; len],
            first_result: -1,
        }
    }

    #[allow(dead_code)]
    pub fn key(&self) -> Option<Arc<[u8]>> {
        if self.first_result < 0 || self.first_result >= (self.keys.len() as i64) {
            return None;
        }
        return self.keys[self.first_result as usize].clone();
    }

    pub fn go_to_min(&mut self) -> DbResult<()> {
        self.first_result = -1;
        let mut idx: usize = 0;
        for cursor in &mut self.cursors {
            cursor.go_to_min()?;

            self.keys[idx] = cursor.key();

            idx += 1;
        }

        self.fin_min_key_and_seek_to_value()
    }

    pub fn seek(&mut self, key: &[u8]) -> DbResult<()> {
        self.first_result = -1;
        let mut idx: usize = 0;

        for cursor in &mut self.cursors {
            let tmp = cursor.seek(key);

            // the key is greater than every keys in the set
            if let Some(Ordering::Greater) = tmp {
                cursor.reset();
            } else {
                self.keys[idx] = cursor.key();
            }

            idx += 1;
        }

        self.fin_min_key_and_seek_to_value()
    }

    /// seek to the min keys in the cursor vec
    fn fin_min_key_and_seek_to_value(&mut self) -> DbResult<()> {
        let mut min_key_idx: i64 = -1;
        let mut min_key: Option<Arc<[u8]>> = None;
        let mut idx = 0;

        while idx < self.keys.len() {
            let this_key = self.keys[idx].clone();
            if min_key.is_none() {
                min_key_idx = idx as i64;
                min_key = this_key;
            } else if this_key.is_some() {
                let min_key_ref = min_key.as_ref().unwrap();
                let this_key_ref = this_key.as_ref().unwrap();
                let cmp = this_key_ref.cmp(min_key_ref);
                if cmp == Ordering::Less {
                    min_key_idx = idx as i64;
                    min_key = this_key;
                }
            }

            idx += 1;
        }

        self.first_result = min_key_idx;

        let changed = self.seed_to_value()?;
        if changed {
            self.fin_min_key_and_seek_to_value()
        } else {
            Ok(())
        }
    }

    /// Skip the deleted value
    ///
    /// The returning boolean value represents if any cursor changes.
    /// Once some cursors changes, we need to find the min key and
    /// push all the following again.
    fn seed_to_value(&mut self) -> DbResult<bool> {
        if self.first_result < 0 {
            return Ok(false)
        }

        let first_fit = self.first_result as usize;

        let fit_marker = self.cursors[first_fit].marker()?.clone();

        match fit_marker {
            None => {
                // The set is empty
                return Ok(false);
            }
            Some(LsmTreeValueMarker::Value(_)) => { return Ok(false) }
            Some(LsmTreeValueMarker::Deleted) => {
                let fit_key = self.keys[first_fit].clone().unwrap();
                self.push_following_cursor_bigger_than(first_fit, &fit_key)?;

                self.cursor_next(first_fit)?;

                Ok(true)
            }
            Some(LsmTreeValueMarker::DeleteStart) => {
                let mut key_base: Option<Arc<[u8]>> = None;
                loop {
                    self.cursors[first_fit].next()?;
                    let key_opt = self.cursors[first_fit].key();
                    let marker = self.cursors[first_fit].marker()?;
                    self.keys[first_fit] = key_opt.clone();
                    match (&key_opt, &marker) {
                        (None, _) => {
                            break;
                        }
                        (Some(key), Some(LsmTreeValueMarker::DeleteEnd)) => {
                            key_base = Some(key.clone());
                            break;
                        }
                        _ => ()  // continue
                    }
                }
                match key_base {
                    Some(key) => {
                        self.push_following_cursor_bigger_than(first_fit, &key)?;

                        self.cursor_next(first_fit)?;

                        Ok(true)
                    }
                    None => {
                        self.reset_following_cursors(first_fit)?;
                        Ok(true)
                    }
                }
            }
            Some(LsmTreeValueMarker::DeleteEnd) => {
                let fit_key = self.keys[first_fit].clone().unwrap();
                self.push_following_cursor_bigger_than(first_fit, &fit_key)?;

                self.cursor_next(first_fit)?;

                Ok(true)
            }
        }
    }

    #[inline]
    fn cursor_next(&mut self, index: usize) -> DbResult<()> {
        self.cursors[index].next()?;
        self.keys[index] = self.cursors[index].key();
        Ok(())
    }

    /// Push all the following cursors bigger than the `index_base`
    /// And make the cursor at `index_base` go next
    fn push_following_cursor_bigger_than(&mut self, index_base: usize, key_base: &Arc<[u8]>) -> DbResult<bool> {
        let mut result = false;
        for idx in (index_base + 1)..(self.cursors.len()) {
            let cursor = &mut self.cursors[idx];

            loop {
                let key = cursor.key();
                if key.is_none() {
                    break;
                }

                let key = key.unwrap();
                let cmp_result = key.cmp(key_base);
                if cmp_result == Ordering::Greater {
                    break;
                } else {
                    cursor.next()?;
                    self.keys[idx] = cursor.key();
                    result = true;
                }
            }
        }

        Ok(result)
    }

    fn reset_following_cursors(&mut self, index_base: usize) -> DbResult<()> {
        for idx in (index_base + 1)..(self.cursors.len()) {
            self.cursors[idx].reset();
        }

        Ok(())
    }

    pub fn value(&self, db: &LsmKvInner) -> DbResult<Option<Arc<[u8]>>> {
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

    pub fn unwrap_tuple_ptr(&self) -> DbResult<LsmTreeValueMarker<LsmTuplePtr>> {
        assert!(self.first_result >= 0);

        let cursor = &self.cursors[self.first_result as usize];
        return Ok(cursor.unwrap_tuple_ptr());
    }

    pub fn next(&mut self) -> DbResult<()> {
        if self.first_result < 0 && self.first_result >= (self.keys.len() as i64) {
            return Ok(());
        }

        let first_fit = self.first_result as usize;

        let current_key = self.keys[first_fit].clone();
        if current_key.is_none() {
            return Ok(())
        }

        let cursor = &mut self.cursors[first_fit];
        cursor.next()?;
        self.keys[first_fit] = cursor.key();

        let mut idx: usize = first_fit + 1;
        while idx < self.keys.len() {
            let this_key = &self.keys[idx as usize];
            if let Some(this_key) = this_key {
                if this_key.cmp(current_key.as_ref().unwrap()) == Ordering::Equal {
                    self.cursors[idx as usize].next()?;
                    self.keys[idx as usize] = self.cursors[idx as usize].key();
                }
            }

            idx += 1;
        }

        self.first_result = -1;
        self.fin_min_key_and_seek_to_value()
    }

    #[allow(dead_code)]
    pub fn done(&self) -> bool {
        for cursor in &self.cursors {
            if !cursor.done() {
                return false;
            }
        }

        true
    }

}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use crate::lsm::lsm_tree::LsmTree;
    use crate::lsm::multi_cursor::MultiCursor;

    #[test]
    fn test_order_of_multi_cursor() {
        let map0 = {
            let mut map = LsmTree::<Arc<[u8]>, Arc<[u8]>>::new();
            map.insert_in_place([10].as_ref().into(), vec![10].into());
            map.insert_in_place([40].as_ref().into(), vec![40].into());
            map.insert_in_place([60].as_ref().into(), vec![60].into());
            map
        };

        let map1 = {
            let mut map = LsmTree::<Arc<[u8]>, Arc<[u8]>>::new();
            map.insert_in_place([20].as_ref().into(), vec![20].into());
            map.insert_in_place([30].as_ref().into(), vec![30].into());
            map.insert_in_place([50].as_ref().into(), vec![50].into());
            map
        };

        let mut cursor = MultiCursor::new(vec![
            map0.open_cursor().into(),
            map1.open_cursor().into(),
        ]);

        cursor.seek(&[10]).unwrap();
        assert_eq!(cursor.key().unwrap().as_ref(), &[10]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap().as_ref(), &[20]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap().as_ref(), &[30]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap().as_ref(), &[40]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap().as_ref(), &[50]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap().as_ref(), &[60]);

        cursor.next().unwrap();
        assert!(cursor.done());
    }

    #[test]
    fn test_deleted_value() {
        let map0 = {
            let mut map = LsmTree::<Arc<[u8]>, Arc<[u8]>>::new();
            map.insert_in_place([10].as_ref().into(), vec![10].into());
            map.insert_in_place([40].as_ref().into(), vec![40].into());
            map.insert_in_place([60].as_ref().into(), vec![60].into());

            map.delete_in_place::<[u8]>([40].as_ref());

            map
        };

        let map1 = {
            let mut map = LsmTree::<Arc<[u8]>, Arc<[u8]>>::new();
            map.insert_in_place([20].as_ref().into(), vec![20].into());
            map.insert_in_place([30].as_ref().into(), vec![30].into());
            map.insert_in_place([50].as_ref().into(), vec![50].into());

            map.delete_in_place::<[u8]>([20].as_ref());

            map
        };

        let mut cursor = MultiCursor::new(vec![
            map0.open_cursor().into(),
            map1.open_cursor().into(),
        ]);

        cursor.seek(&[10]).unwrap();
        assert_eq!(cursor.key().unwrap().as_ref(), &[10]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap().as_ref(), &[30]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap().as_ref(), &[50]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap().as_ref(), &[60]);

        cursor.next().unwrap();
        assert!(cursor.done());
    }

}
