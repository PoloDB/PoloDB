/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;
use bson::oid::ObjectId;
use crate::lsm::lsm_tree::LsmTree;

const TIMESTAMP_SIZE: usize = 4;
const PROCESS_ID_SIZE: usize = 5;
const COUNTER_SIZE: usize = 3;

const TIMESTAMP_OFFSET: usize = 0;
const PROCESS_ID_OFFSET: usize = TIMESTAMP_OFFSET + TIMESTAMP_SIZE;
const COUNTER_OFFSET: usize = PROCESS_ID_OFFSET + PROCESS_ID_SIZE;

#[derive(Copy, Clone)]
pub(crate) struct LsmTuplePtr {
    pub pid:       u64,
    pub offset:    u32,
    pub byte_size: u64,
}

impl LsmTuplePtr {

    // self.pid
    fn get_oid_timestamp(&self) -> [u8; 4] {
        let mut buffer = [0u8; 4];
        let t_buffer: [u8; 8] = self.pid.to_be_bytes();
        buffer.copy_from_slice(&t_buffer[0..4]);
        buffer
    }

    // self.byte_size
    fn get_oid_pid(&self) -> [u8; 5] {
        let mut buffer = [0u8; 5];
        let pid_buffer: [u8; 8] = self.byte_size.to_be_bytes();
        buffer.copy_from_slice(&pid_buffer[0..5]);
        buffer
    }

    // self.offset
    fn get_oid_counter(&self) -> [u8; 3] {
        let mut buffer = [0u8; 3];
        let counter_buffer: [u8; 4] = self.offset.to_be_bytes();
        buffer.copy_from_slice(&counter_buffer[0..3]);
        buffer
    }

}

impl Default for LsmTuplePtr {

    fn default() -> Self {
        LsmTuplePtr {
            pid: 0,
            offset: 0,
            byte_size: 0,
        }
    }

}

// Immutable segment
#[derive(Clone)]
pub(crate) struct ImLsmSegment {
    pub segments:         LsmTree<Arc<[u8]>, LsmTuplePtr>,
    pub start_pid:        u64,
    pub end_pid:          u64,
}

impl ImLsmSegment {

    #[allow(dead_code)]
    pub fn from_object_id(oid: &ObjectId) -> ImLsmSegment {
        let bytes = oid.bytes();

        let mut start_bytes = [0u8; 8];
        let mut end_bytes = [0u8; 8];

        start_bytes.copy_from_slice(&bytes[0..8]);
        end_bytes[0..4].copy_from_slice(&bytes[8..12]);

        ImLsmSegment {
            segments: LsmTree::new(),
            start_pid: u64::from_be_bytes(start_bytes),
            end_pid: u64::from_be_bytes(end_bytes),
        }
    }

    #[allow(dead_code)]
    pub fn to_object_id(&self) -> ObjectId {
        let mut bytes = [0u8; 12];

        let start_be: [u8; 8] = self.start_pid.to_be_bytes();
        let end_be: [u8; 8] = self.end_pid.to_be_bytes();

        bytes[0..8].copy_from_slice(&start_be);
        bytes[8..12].copy_from_slice(&end_be[0..4]);

        ObjectId::from_bytes(bytes)
    }

}

#[cfg(test)]
mod test {
    use bson::oid::ObjectId;
    use crate::lsm::lsm_segment::{ImLsmSegment, LsmTuplePtr};

    #[test]
    fn test_oid_conversion() {
        for _ in 0..100 {
            let oid = ObjectId::new();
            let ptr = ImLsmSegment::from_object_id(&oid);
            let back = ptr.to_object_id();
            assert_eq!(oid, back, "oid: {}, back: {}", oid, back);
        }
    }

}
