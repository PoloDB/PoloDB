/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};
use crate::lsm::lsm_snapshot::LsmMetaDelegate;
use crate::lsm::lsm_segment::ImLsmSegment;
use crate::page::RawPage;

#[derive(Clone)]
pub(crate) struct LsmLevel {
    pub age:     u16,
    pub content: SmallVec<[ImLsmSegment; 4]>,
}

impl LsmLevel {

    fn new() -> LsmLevel {
        LsmLevel {
            age: 0,
            content: smallvec![],
        }
    }

    pub fn clear_except_last(&mut self) {
        self.content = smallvec![self.content.last().unwrap().clone()];
    }

}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub(crate) struct  FreeSegmentRecord {
    pub start_pid: u64,
    pub end_pid: u64,
}

#[derive(Clone)]
pub(crate) struct LsmSnapshot {
    pub meta_pid:              u8,   // The page id of the meta page
    /// Incremental counter.
    /// Default is 1, so it's bigger than null(0)
    pub meta_id:               u64,
    pub file_size:             u64,
    pub log_offset:            u64,
    pub levels:                Vec<LsmLevel>,
    pub free_segments:         Vec<FreeSegmentRecord>,
    pub pending_free_segments: Vec<FreeSegmentRecord>,
}

impl LsmSnapshot {

    pub fn new() -> LsmSnapshot {
        LsmSnapshot {
            meta_pid: 0,
            meta_id: 1,
            file_size: 0,
            log_offset: 0,
            free_segments: Vec::with_capacity(4),
            levels: Vec::with_capacity(4),
            pending_free_segments: Vec::new(),
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

    pub fn next_meta_pid(&self) -> u8 {
        if self.meta_pid == 0 {
            1
        } else {
            0
        }
    }

    pub fn write_to_page(&self, page: &mut RawPage) {
        let mut delegate = LsmMetaDelegate(page);
        delegate.set_meta_id(self.meta_id);
        delegate.set_log_offset(self.log_offset);

        assert!(self.levels.len() < u8::MAX as usize);
        delegate.set_level_count(self.levels.len() as u8);

        delegate.begin_write_level();
        for level in &self.levels {
            delegate.write_level(level);
        }

        delegate.write_free_segments(&self.free_segments);
    }

    pub fn flush_pending_segments(&mut self) {
        for seg in &self.pending_free_segments {
            self.free_segments.push(seg.clone());
        }
        self.pending_free_segments.clear();
    }

    pub fn normalize_free_segments(&mut self) {
        if self.free_segments.is_empty() {
            return;
        }

        self.free_segments.sort_by(|a, b| {
            a.start_pid.cmp(&b.start_pid)
        });

        let mut index: usize = 0;

        while index < self.free_segments.len() - 1 {
            let (next_start_pid, next_end_pid) = {
                let next = &self.free_segments[index + 1];
                (next.start_pid, next.end_pid)
            };
            let this = &mut self.free_segments[index];

            if this.end_pid + 1 == next_start_pid {
                this.end_pid = next_end_pid;
                self.free_segments.remove(index + 1);
            } else {
                index += 1;
            }
        }
    }

}
