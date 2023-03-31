use std::cmp::Ordering;
use std::sync::Arc;
use crate::lsm::lsm_tree::TreeCursor;
use super::lsm_tree::LsmTree;

#[derive(Clone)]
pub(crate) struct MemTable {
    segments:         LsmTree<Arc<[u8]>, Arc<[u8]>>,
    store_bytes:      usize,
}

impl MemTable {

    pub fn new() -> MemTable {
        MemTable {
            segments: LsmTree::new(),
            store_bytes: 0,
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Arc<[u8]>> {
        let mut cursor = self.segments.open_cursor();
        let ord  = cursor.seek(key)?;
        if ord == Ordering::Equal {
            cursor
                .value()
                .map(|marker| marker.into())
                .flatten()
        } else {
            None
        }
    }

    pub fn put<K, V>(&mut self, key: K, value: V, in_place: bool)
    where
        K: Into<Arc<[u8]>>,
        V: Into<Arc<[u8]>>,
    {
        let key = key.into();
        let value = value.into();
        let key_len = key.len();
        let value_len = value.len();

        let prev = if in_place {
            let prev = self.segments.insert_in_place(key, value);
            prev
        } else {
            let prev = self.get(key.as_ref());

            self.segments = self.segments.insert(key, value);

            prev
        };

        if let Some(prev) = prev {
            self.store_bytes -= prev.len();
            self.store_bytes += value_len;
        } else {
            self.store_bytes += 1;  // for the flag
            self.store_bytes += key_len;
            self.store_bytes += value_len;
        }
    }

    /// Store will not really delete the value
    /// But inert a flag
    pub fn delete<K>(&mut self, key: K, in_place: bool)
    where
        K: AsRef<[u8]>
    {
        let prev = if in_place {
            self.segments.delete_in_place(key.as_ref())
        } else {
            let prev = self.get(key.as_ref());
            self.segments = self.segments.delete(key.as_ref().into());
            prev
        };

        if let Some(prev) = prev {
            self.store_bytes -= prev.len();
        }
    }

    #[inline]
    pub fn store_bytes(&self) -> usize {
        self.store_bytes
    }

    #[inline]
    pub fn open_cursor(&self) -> TreeCursor<Arc<[u8]>, Arc<[u8]>> {
        self.segments.open_cursor()
    }

    pub(crate) fn clear(&mut self) {
        self.segments.clear();
        self.store_bytes = 0;
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.segments.len()
    }

}
