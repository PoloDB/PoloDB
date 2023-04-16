/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;
use bson::oid::ObjectId;
use crate::lsm::lsm_tree::LsmTree;

#[derive(Copy, Clone)]
#[allow(dead_code)]
pub(crate) struct LsmTuplePtr {
    pub pid:       u64,
    pub pid_ext:   u32,   // reserved for ObjectId
    pub offset:    u32,
    pub byte_size: u64,
}

impl LsmTuplePtr {

    #[allow(dead_code)]
    pub fn from_object_id(oid: &ObjectId, offset: u32, byte_size: u64) -> LsmTuplePtr {
        let mut pid_be = [0u8; 8];
        let mut pid_ext_be = [0u8; 4];
        let oid_bytes = oid.bytes();

        pid_be.copy_from_slice(&oid_bytes[0..8]);
        pid_ext_be.copy_from_slice(&oid_bytes[8..12]);

        let pid = u64::from_be_bytes(pid_be);
        let pid_ext = u32::from_be_bytes(pid_ext_be);

        LsmTuplePtr {
            pid,
            pid_ext,
            offset,
            byte_size,
        }
    }

}

impl Default for LsmTuplePtr {

    fn default() -> Self {
        LsmTuplePtr {
            pid: 0,
            pid_ext: 0,
            offset: 0,
            byte_size: 0,
        }
    }

}

// Immutable segment
#[derive(Clone)]
pub(crate) struct ImLsmSegment {
    pub segments:  LsmTree<Arc<[u8]>, LsmTuplePtr>,
    pub start_pid: u64,
    pub end_pid:   u64,
}

impl ImLsmSegment {

    #[allow(dead_code)]
    pub fn from_object_id(
        segments:  LsmTree<Arc<[u8]>, LsmTuplePtr>,
        oid: &ObjectId,
    ) -> ImLsmSegment {
        let bytes = oid.bytes();

        let mut start_bytes = [0u8; 8];
        let mut end_bytes = [0u8; 8];

        start_bytes.copy_from_slice(&bytes[0..8]);
        end_bytes[0..4].copy_from_slice(&bytes[8..12]);

        ImLsmSegment {
            segments,
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
    use crate::lsm::lsm_segment::{ImLsmSegment};
    use crate::lsm::lsm_tree::LsmTree;

    #[test]
    fn test_oid_conversion() {
        for _ in 0..100 {
            let oid = ObjectId::new();
            let ptr = ImLsmSegment::from_object_id(
                LsmTree::new(),
                &oid,
            );
            let back = ptr.to_object_id();
            assert_eq!(oid, back, "oid: {}, back: {}", oid, back);
        }
    }

}
