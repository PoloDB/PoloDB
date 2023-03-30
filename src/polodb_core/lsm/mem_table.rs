use std::sync::Arc;
use crate::lsm::lsm_tree::TreeCursor;
use super::lsm_tree::LsmTree;

pub(crate) struct MemTable {
    segments:         LsmTree<Arc<[u8]>, Vec<u8>>,
    store_bytes:      usize,
}

impl MemTable {

    pub fn new() -> MemTable {
        MemTable {
            segments: LsmTree::new(),
            store_bytes: 0,
        }
    }

    pub fn put<K, V>(&mut self, key: K, value: V)
    where
        K: Into<Arc<[u8]>>,
        V: Into<Vec<u8>>,
    {
        let key = key.into();
        let value = value.into();
        let key_len = key.len();
        let value_len = value.len();

        let prev = self.segments.insert_in_place(key, value);

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
    pub fn delete<K>(&mut self, key: K)
    where
        K: AsRef<[u8]>
    {
        let prev = self.segments.delete_in_place(key.as_ref());

        if let Some(prev) = prev {
            self.store_bytes -= prev.len();
        }
    }

    #[inline]
    pub fn store_bytes(&self) -> usize {
        self.store_bytes
    }

    #[inline]
    pub fn open_cursor(&self) -> TreeCursor<Arc<[u8]>, Vec<u8>> {
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
