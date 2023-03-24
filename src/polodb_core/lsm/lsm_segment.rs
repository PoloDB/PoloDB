/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;
use crate::lsm::lsm_tree::LsmTree;

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

// Immutable segment
#[derive(Clone)]
pub(crate) struct ImLsmSegment {
    pub segments:         LsmTree<Arc<[u8]>, LsmTuplePtr>,
    pub start_pid:        u64,
    pub end_pid:          u64,
}
