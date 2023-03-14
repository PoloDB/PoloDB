/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use im::OrdMap;

#[derive(Copy, Clone)]
pub(crate) struct LsmSegmentPtr {
    pub start_pid: u64,
    pub end_pid: u64,
}

impl Default for LsmSegmentPtr {
    fn default() -> Self {
        LsmSegmentPtr {
            start_pid: 0,
            end_pid: 0,
        }
    }
}

#[derive(Clone)]
pub(crate) enum SegValue {
    Deleted,
    OwnValue(Vec<u8>),
}

impl SegValue {

    pub fn len(&self) -> usize {
        match self {
            SegValue::Deleted => 0,
            SegValue::OwnValue(bytes) => bytes.len()
        }
    }

}

// Immutable segment
#[derive(Clone)]
pub(crate) struct ImLsmSegment {
    pub segments:      OrdMap<Vec<u8>, LsmSegmentPtr>,
    store_bytes:       usize,
    left_segment_pid:  u64,
}

pub(crate) struct LsmSegment {
    pub segments:      OrdMap<Vec<u8>, SegValue>,
    store_bytes:       usize,
    left_segment_pid:  u64,
}

impl LsmSegment {

    pub fn new(left_segment_pid: u64) -> LsmSegment {
        LsmSegment {
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
