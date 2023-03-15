/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use im::OrdMap;

#[derive(Copy, Clone)]
pub(crate) struct LsmTuplePtr {
    pub pid:    u64,
    pub offset: u32,
}

impl Default for LsmTuplePtr {
    fn default() -> Self {
        LsmTuplePtr {
            pid: 0,
            offset: 0,
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
    pub segments:         OrdMap<Vec<u8>, LsmTuplePtr>,
    pub start_pid:        u64,
    pub end_pid:          u64,
}
