use im::OrdMap;
use crate::lsm::lsm_segment::SegValue;

pub(crate) struct MemTable {
    pub segments:      OrdMap<Vec<u8>, SegValue>,
    store_bytes:       usize,
    left_segment_pid:  u64,
}

impl MemTable {

    pub fn new(left_segment_pid: u64) -> MemTable {
        MemTable {
            segments: OrdMap::new(),
            store_bytes: 0,
            left_segment_pid,
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        let prev = self.segments.insert(key.into(), SegValue::OwnValue(value.into()));

        if let Some(prev) = prev {
            self.store_bytes -= prev.len();
            self.store_bytes += value.len();
        } else {
            self.store_bytes += 1;  // for the flag
            self.store_bytes += key.len();
            self.store_bytes += value.len();
        }
    }

    /// Store will not really delete the value
    /// But inert a flag
    pub fn delete(&mut self, key: &[u8]) {
        let prev = self.segments.insert(key.into(), SegValue::Deleted);

        if let Some(prev) = prev {
            self.store_bytes -= prev.len();
        }
    }

    #[inline]
    pub fn store_bytes(&self) -> usize {
        self.store_bytes
    }

}
