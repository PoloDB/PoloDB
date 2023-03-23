/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::io::Read;
use byteorder::ReadBytesExt;
use memmap2::Mmap;
use smallvec::{SmallVec, smallvec};
use crate::{DbErr, DbResult};
use crate::lsm::lsm_segment::{ImLsmSegment, LsmTuplePtr};
use crate::lsm::lsm_snapshot::lsm_meta::{DB_FILE_SIZE_OFFSET, LEVEL_COUNT_OFFSET, LOG_OFFSET_OFFSET};
use crate::lsm::lsm_snapshot::{LsmLevel, LsmSnapshot};
use crate::lsm::lsm_tree::{LsmTree, LsmTreeValueMarker};
use super::format;

pub(crate) struct SnapshotReader<'a> {
    mmap: &'a Mmap,
    page_size: u32,
}

impl<'a> SnapshotReader<'a> {

    pub fn new(mmap: &'a Mmap, page_size: u32) -> SnapshotReader {
        SnapshotReader {
            mmap,
            page_size,
        }
    }

    pub fn read_snapshot_from(&self, meta_pid: u8, meta_id: u64) -> DbResult<LsmSnapshot> {
        let meta_start_offset = (meta_pid as usize) * (self.page_size as usize);
        let meta_slice = &self.mmap[meta_start_offset..(meta_start_offset + self.page_size as usize)];

        let mut db_file_size_be: [u8; 8] = [0; 8];
        db_file_size_be.copy_from_slice(&meta_slice[(DB_FILE_SIZE_OFFSET as usize)..((DB_FILE_SIZE_OFFSET + 8) as usize)]);
        let db_file_size = u64::from_be_bytes(db_file_size_be);

        let mut log_offset_be: [u8; 8] = [0; 8];
        log_offset_be.copy_from_slice(&meta_slice[(LOG_OFFSET_OFFSET as usize)..((LOG_OFFSET_OFFSET + 8) as usize)]);
        let log_offset = u64::from_be_bytes(log_offset_be);

        let levels = self.read_level_from_page(meta_slice)?;

        let result = LsmSnapshot {
            meta_pid,
            meta_id,
            file_size: db_file_size,
            log_offset,
            levels,
            free_segments: Vec::with_capacity(4),
        };

        Ok(result)
    }

    fn read_level_from_page(&self, meta_slice: &[u8]) -> DbResult<Vec<LsmLevel>> {
        let level_count = meta_slice[LEVEL_COUNT_OFFSET as usize] as usize;
        let mut levels = Vec::with_capacity(level_count);

        let mut ptr = 128;

        for _ in 0..level_count {
            let mut level_age_be: [u8; 2] = [0; 2];
            level_age_be.copy_from_slice(&meta_slice[ptr..(ptr + 2)]);

            let level_age = u16::from_be_bytes(level_age_be);
            ptr += 2;

            let level_len = meta_slice[ptr] as usize;
            ptr += 1;

            // preserved
            ptr += 1;

            let mut level_content: SmallVec<[ImLsmSegment; 4]> = smallvec![];

            for _ in 0..level_len {
                let mut start_pid_be: [u8; 8] = [0; 8];
                start_pid_be.copy_from_slice(&meta_slice[ptr..(ptr + 8)]);

                let start_pid = u64::from_be_bytes(start_pid_be);
                ptr += 8;

                let mut end_pid_be: [u8; 8] = [0; 8];
                end_pid_be.copy_from_slice(&meta_slice[ptr..(ptr + 8)]);

                let end_pid = u64::from_be_bytes(end_pid_be);
                ptr += 8;

                let mut tuple_len_be: [u8; 8] = [0; 8];
                tuple_len_be.copy_from_slice(&meta_slice[ptr..(ptr + 8)]);

                let tuple_len = u64::from_be_bytes(tuple_len_be);
                ptr += 8;

                // preserved
                ptr += 8;

                let segment = self.read_segment(
                    start_pid,
                    end_pid,
                    tuple_len,
                )?;

                level_content.push(segment)
            }

            let level = LsmLevel {
                age: level_age,
                content: level_content,
            };
            levels.push(level);
        }

        Ok(levels)
    }

    fn read_segment(&self, start_pid: u64, end_pid: u64, tuple_len: u64) -> DbResult<ImLsmSegment> {
        let start_offset = (start_pid as usize) * (self.page_size as usize);
        let end_offset = (end_pid + 1) as usize * (self.page_size as usize);
        let mut segment_slice = &self.mmap[start_offset..end_offset];
        let mut segments = LsmTree::new();

        for _ in 0..tuple_len {
            let global_offset = segment_slice.as_ptr() as usize - self.mmap.as_ptr() as usize;
            let pid = global_offset / (self.page_size as usize);
            let offset = global_offset % (self.page_size as usize);

            let flag = segment_slice.read_u8()?;

            let key_len = crate::btree::vli::decode_u64(&mut segment_slice)?;
            let mut key_buffer = vec![0; key_len as usize];
            segment_slice.read_exact(&mut key_buffer)?;

            match flag {
                format::LSM_INSERT => {
                    let value_len = crate::btree::vli::decode_u64(&mut segment_slice)?;
                    segment_slice = &segment_slice[(value_len as usize)..];

                    segments.insert_in_place(
                        key_buffer.into_boxed_slice(),
                        LsmTuplePtr {
                            pid: pid as u64,
                            offset: offset as u32,
                        },
                    );
                }
                format::LSM_POINT_DELETE => {
                    segments.update_in_place(
                        key_buffer.into_boxed_slice(),
                        LsmTreeValueMarker::Deleted,
                    );
                }
                format::LSM_START_DELETE => {
                    segments.update_in_place(
                        key_buffer.into_boxed_slice(),
                        LsmTreeValueMarker::DeleteStart,
                    );
                }
                format::LSM_END_DELETE => {
                    segments.update_in_place(
                        key_buffer.into_boxed_slice(),
                        LsmTreeValueMarker::DeleteEnd,
                    );
                }
                _ => {
                    return Err(DbErr::DataMalformed);
                }
            }
        }

        Ok(ImLsmSegment {
            segments,
            start_pid,
            end_pid,
        })
    }

}
