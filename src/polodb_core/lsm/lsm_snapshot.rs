/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;
use super::lsm_segment::ImLsmSegment;

#[derive(Clone)]
pub(crate) struct LsmLevel {
    age:     u64,
    content: Vec<ImLsmSegment>,
    len:     usize,
}

impl LsmLevel {

    fn new() -> LsmLevel {
        LsmLevel {
            age: 0,
            content: vec![],
            len: 0,
        }
    }

}

#[derive(Clone)]
pub(crate) struct LsmSnapshot {
    free_blocks:     Vec<u32>,
    levels:          Vec<Arc<LsmLevel>>,
    pub segment_pid: u64,
}

impl LsmSnapshot {

    #[allow(dead_code)]
    pub fn new() -> LsmSnapshot {
        LsmSnapshot {
            free_blocks: Vec::with_capacity(4),
            levels: Vec::with_capacity(4),
            segment_pid: 0,
        }
    }

}
