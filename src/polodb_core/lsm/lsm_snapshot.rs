/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use super::lsm_segment::ImLsmSegment;
use smallvec::{SmallVec, smallvec};

#[derive(Clone)]
pub(crate) struct LsmLevel {
    pub age:     u16,
    pub content: SmallVec<[ImLsmSegment; 4]>,
    pub len:     usize,
}

impl LsmLevel {

    fn new() -> LsmLevel {
        LsmLevel {
            age: 0,
            content: smallvec![],
            len: 0,
        }
    }

}

#[derive(Clone)]
pub(crate) struct LsmSnapshot {
    pub meta_pid:    i8,   // The page id of the meta page
    pub meta_id:     u64,  // Incremental counter
    pub pid_ptr:     u64,  // pid of current writer
    pub log_offset:  u64,
    pub free_blocks: Vec<u32>,
    pub levels:      Vec<LsmLevel>,
}

impl LsmSnapshot {

    pub fn new() -> LsmSnapshot {
        LsmSnapshot {
            meta_pid: 1,
            meta_id: 1,
            pid_ptr: 2,
            log_offset: 0,
            free_blocks: Vec::with_capacity(4),
            levels: Vec::with_capacity(4),
        }
    }

    pub fn add_latest_segment(&mut self, segment: ImLsmSegment) {
        if self.levels.is_empty() {
            let mut level0 = LsmLevel::new();
            level0.content.push(segment);
            self.levels.push(level0);
            return;
        }
        let level0 = &mut self.levels[0];
        level0.content.push(segment);
    }

    pub fn consume_free_blocks(&mut self) -> u32 {
        let last = *self.free_blocks.last().unwrap();
        self.free_blocks.remove(self.free_blocks.len() - 1);
        last
    }

    pub fn segment_pid(&self) -> u64 {
        if self.levels.is_empty() {
            return 0;
        }
        let level0 = &self.levels[0];
        let last_at_level = level0.content.last();
        match last_at_level {
            Some(segment) => segment.start_pid,
            None => 0,
        }
    }

    pub fn next_meta_pid(&mut self) -> i8 {
        if self.meta_pid == 0 {
            1
        } else {
            0
        }
    }

}
