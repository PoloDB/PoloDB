/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::{Mutex, Arc};
use byteorder::ReadBytesExt;
use memmap2::Mmap;
use smallvec::smallvec;
use crate::{Config, DbErr, DbResult};
use crate::lsm::lsm_backend::file_writer::FileWriter;
use crate::lsm::lsm_backend::format;
use crate::lsm::lsm_backend::lsm_backend::{lsm_backend_utils, LsmBackend};
use crate::lsm::lsm_backend::snapshot_reader::SnapshotReader;
use crate::lsm::mem_table::MemTable;
use crate::lsm::lsm_segment::{ImLsmSegment, LsmTuplePtr};
use crate::lsm::lsm_snapshot::{FreeSegmentRecord, LsmLevel, LsmMetaDelegate, LsmSnapshot};
use crate::page::RawPage;
use crate::lsm::lsm_tree::{LsmTree, LsmTreeValueMarker};
use crate::lsm::lsm_snapshot::lsm_meta::{META_ID_OFFSET};
use crate::lsm::LsmMetrics;
use crate::lsm::multi_cursor::{CursorRepr, MultiCursor};
use crate::utils::vli;

#[cfg(target_os = "windows")]
mod winerror {
    pub const ERROR_SHARING_VIOLATION: i32 = 32;
}

#[cfg(target_os = "windows")]
fn open_file_native(path: &Path) -> DbResult<File> {
    use std::os::windows::prelude::OpenOptionsExt;

    let file_result = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .share_mode(0)
        .open(path);

    match file_result {
        Ok(file) => Ok(file),
        Err(err) => {
            let os_error = err.raw_os_error();
            if let Some(error_code) = os_error {
                if error_code == winerror::ERROR_SHARING_VIOLATION {
                    return Err(DbErr::DatabaseOccupied);
                }
            }
            Err(err.into())
        }
    }
}

#[cfg(not(target_os = "windows"))]
fn open_file_native(path: &Path) -> DbResult<File> {
    use crate::utils::file_lock::exclusive_lock_file;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(path)?;

    match exclusive_lock_file(&file) {
        Err(DbErr::Busy) => {
            return Err(DbErr::DatabaseOccupied);
        }
        Err(err) => {
            return Err(err);
        },
        _ => (),
    };

    Ok(file)
}

pub(crate) struct LsmFileBackend {
    inner: Mutex<LsmFileBackendInner>,
}

impl LsmFileBackend {

    #[allow(unused)]
    pub fn open(
        path: &Path,
        metrics: LsmMetrics,
        config: Config,
    ) -> DbResult<LsmFileBackend> {
        let inner = LsmFileBackendInner::open(path, metrics, config)?;
        Ok(LsmFileBackend {
            inner: Mutex::new(inner),
        })
    }

}

impl LsmBackend for LsmFileBackend {

    fn read_segment_by_ptr(&self, ptr: LsmTuplePtr) -> DbResult<Arc<[u8]>> {
        let mut inner = self.inner.lock()?;
        inner.read_segment_by_ptr(ptr)
    }

    fn read_latest_snapshot(&self) -> DbResult<LsmSnapshot> {
        let mut inner = self.inner.lock()?;
        inner.read_latest_snapshot()
    }

    fn sync_latest_segment(&self, segment: &MemTable, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        inner.sync_latest_segment(segment, snapshot)
    }

    fn minor_compact(&self, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        inner.minor_compact(snapshot)
    }

    fn major_compact(&self, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        inner.major_compact(snapshot)
    }

    fn checkpoint_snapshot(&self, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        inner.checkpoint_snapshot(snapshot)
    }

}

struct LsmFileBackendInner {
    file:    File,
    metrics: LsmMetrics,
    config:  Config,
}

impl LsmFileBackendInner {

    fn open(
        path: &Path,
        metrics: LsmMetrics,
        config: Config,
    ) -> DbResult<LsmFileBackendInner> {
        let file = open_file_native(path)?;
        Ok(LsmFileBackendInner {
            file,
            metrics,
            config,
        })
    }

    fn force_init_file(&mut self) -> DbResult<LsmSnapshot> {
        let mut result = LsmSnapshot::new();
        let mut first_page = RawPage::new(0, NonZeroU32::new(self.config.get_lsm_page_size()).unwrap());

        let _delegate = LsmMetaDelegate::new_with_default(&mut first_page);

        first_page.sync_to_file(&mut self.file, 0)?;
        self.file.flush()?;

        let page_size = self.config.get_lsm_page_size();
        let meta_size = (page_size * 2) as u64;

        self.file.set_len(meta_size)?;
        self.file.seek(SeekFrom::End(0))?;

        result.file_size = meta_size;

        Ok(result)
    }

    fn read_segment_by_ptr(&mut self, tuple: LsmTuplePtr) -> DbResult<Arc<[u8]>> {
        let page_size = self.config.get_lsm_page_size();
        let offset = (tuple.pid as u64) * (page_size as u64) + (tuple.offset as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        let flag = self.file.read_u8()?;
        assert!(flag == format::LSM_INSERT || flag == format::LSM_POINT_DELETE);

        let key_len = vli::decode_u64(&mut self.file)?;
        self.file.seek(SeekFrom::Current(key_len as i64))?;

        let value_len = vli::decode_u64(&mut self.file)?;

        let mut buffer = vec![0u8; value_len as usize];
        self.file.read_exact(&mut buffer)?;

        Ok(buffer.into())
    }

    fn check_first_page_valid(data: &[u8]) -> DbResult<()> {
        let mut title_area: [u8; 32] = [0; 32];
        if data.len() < 32 {
            return Err(DbErr::NotAValidDatabase);
        }
        title_area.copy_from_slice(&data[0..32]);

        match std::str::from_utf8(&title_area) {
            Ok(s) => {
                if !s.starts_with("PoloDB") {
                    return Err(DbErr::NotAValidDatabase);
                }
                Ok(())
            },
            Err(_) => Err(DbErr::NotAValidDatabase),
        }
    }


    fn read_latest_snapshot(&mut self) -> DbResult<LsmSnapshot> {
        let meta = self.file.metadata()?;
        if meta.len() == 0 { // new file
            return self.force_init_file();
        }

        let mmap = unsafe {
            Mmap::map(&self.file)?
        };

        LsmFileBackendInner::check_first_page_valid(&mmap)?;

        let page_size = self.config.get_lsm_page_size();

        assert!(mmap.len() >= (page_size * 2) as usize);

        let mut meta_offset = META_ID_OFFSET as usize;
        let mut meta1_be: [u8; 8] = [0; 8];
        meta1_be.copy_from_slice(&mmap[meta_offset..(meta_offset + 8)]);

        let mut meta2_be: [u8; 8] = [0; 8];
        meta_offset += page_size as usize;
        meta2_be.copy_from_slice(&mmap[meta_offset..(meta_offset + 8)]);

        let meta1 = u64::from_be_bytes(meta1_be);
        let meta2 = u64::from_be_bytes(meta2_be);

        let reader = SnapshotReader::new(
            &mmap,
            page_size,
        );

        let snapshot = if meta1 > meta2 {
            reader.read_snapshot_from(0, meta1)?
        } else {
            reader.read_snapshot_from(1, meta2)?
        };

        self.metrics.set_free_segments_count(snapshot.free_segments.len());

        Ok(snapshot)
    }

    /// Sync the `mem_table` to the disk,
    /// Add the segment on the top of level 0
    fn sync_latest_segment(&mut self, mem_table: &MemTable, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let config = self.config.clone();

        let estimate_size = LsmFileBackendInner::estimate_mem_table_byte_size(mem_table);
        let (start_pid, used_free_segment) = self.get_start_writing_pid(snapshot, estimate_size);

        let mut writer = FileWriter::open(
            &mut self.file,
            start_pid,
            config,
        );

        writer.begin()?;

        let mut segments = LsmTree::<Arc<[u8]>, LsmTuplePtr>::new();

        let mut mem_table_cursor = mem_table.open_cursor();
        mem_table_cursor.go_to_min();

        while !mem_table_cursor.done() {
            let (key, value) = mem_table_cursor.tuple().unwrap();
            let pos = writer.write_tuple(key.as_ref(), value.as_ref())?;

            segments.update_in_place(key, pos);

            mem_table_cursor.next();
        }

        assert_eq!(writer.written_bytes(), estimate_size as u64);

        let end_ptr = writer.end()?;

        let im_seg = ImLsmSegment {
            segments,
            start_pid,
            end_pid: end_ptr.pid,
        };

        LsmFileBackendInner::return_used_segment(used_free_segment.as_ref(), end_ptr.pid, snapshot);

        snapshot.add_latest_segment(im_seg);
        snapshot.file_size = self.file.seek(SeekFrom::End(0))?;

        Ok(())
    }

    fn return_used_segment(used_segment: Option<&FreeSegmentRecord>, end_pid: u64, snapshot: &mut LsmSnapshot) {
        if let Some(used_segment) = &used_segment {
            if end_pid < used_segment.end_pid {
                snapshot.free_segments.push(FreeSegmentRecord {
                    start_pid: end_pid + 1,
                    end_pid: used_segment.end_pid,
                })
            }
        }
    }

    fn minor_compact(&mut self, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let new_segment = self.merge_level0_except_last(snapshot)?;

        self.insert_new_segment_to_right_level(new_segment, snapshot);

        self.free_pages_of_level0_except_last(snapshot)?;

        snapshot.levels[0].clear_except_last();
        snapshot.levels[0].age += 1;

        LsmFileBackendInner::normalize_free_segments(snapshot)?;

        snapshot.file_size = self.file.seek(SeekFrom::End(0))?;

        Ok(())
    }

    fn major_compact(&mut self, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        assert!(snapshot.levels.len() > 3);
        let new_segment = self.merge_last_two_levels(snapshot)?;

        let mut level_len = snapshot.levels.len();
        let last2 = &snapshot.levels[level_len - 2];
        let last1 = &snapshot.levels[level_len - 1];
        snapshot.free_segments.push(FreeSegmentRecord {
            start_pid: last2.content[0].start_pid,
            end_pid: last2.content[0].end_pid,
        });
        snapshot.free_segments.push(FreeSegmentRecord {
            start_pid: last1.content[0].start_pid,
            end_pid: last1.content[0].end_pid,
        });

        snapshot.levels.remove(level_len - 1);
        level_len -= 1;
        snapshot.levels[level_len - 1] = LsmLevel {
            age: 0,
            content: smallvec![new_segment],
        };

        LsmFileBackendInner::normalize_free_segments(snapshot)?;
        snapshot.file_size = self.file.seek(SeekFrom::End(0))?;

        Ok(())
    }

    fn merge_last_two_levels(&mut self, snapshot: &mut LsmSnapshot) -> DbResult<ImLsmSegment> {
        let level_len = snapshot.levels.len();
        let last2 = &snapshot.levels[level_len - 2];
        let last1 = &snapshot.levels[level_len - 1];

        let cursor = {
            let cursor_repo: Vec<CursorRepr> = vec![
                last2.content[0].segments.open_cursor().into(),
                last1.content[0].segments.open_cursor().into(),
            ];

            MultiCursor::new(cursor_repo)
        };

        let segment = self.merge_level(snapshot, cursor, false)?;

        Ok(segment)
    }

    fn normalize_free_segments(snapshot: &mut LsmSnapshot) -> DbResult<()> {
        snapshot.free_segments.sort_by(|a, b| {
            a.start_pid.cmp(&b.start_pid)
        });

        let mut index: usize = 0;

        while index < snapshot.free_segments.len() - 1 {
            let (next_start_pid, next_end_pid) = {
                let next = &snapshot.free_segments[index + 1];
                (next.start_pid, next.end_pid)
            };
            let this = &mut snapshot.free_segments[index];

            if this.end_pid + 1 == next_start_pid {
                this.end_pid = next_end_pid;
                snapshot.free_segments.remove(index + 1);
            } else {
                index += 1;
            }
        }

        Ok(())
    }

    fn insert_new_segment_to_right_level(&self, new_segment: ImLsmSegment, snapshot: &mut LsmSnapshot) {
        let new_level = LsmLevel {
            age: 0,
            content: smallvec![new_segment],
        };
        snapshot.levels.insert(1, new_level);
    }

    fn free_pages_of_level0_except_last(&self, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let level0 = &snapshot.levels[0];

        let mut index: usize = 0;

        while index < level0.content.len() - 1 {
            let segment = &level0.content[index];
            snapshot.free_segments.push(FreeSegmentRecord {
                start_pid: segment.start_pid,
                end_pid: segment.end_pid,
            });
            index += 1;
        }

        self.metrics.set_free_segments_count(snapshot.free_segments.len());

        Ok(())
    }

    fn merge_level0_except_last(&mut self, snapshot: &mut LsmSnapshot) -> DbResult<ImLsmSegment> {
        let level0 = &snapshot.levels[0];
        assert!(level0.content.len() > 1);

        let preserve_delete = snapshot.levels.len() > 1;

        let cursor = {
            let mut cursor_repo: Vec<CursorRepr> = vec![];
            let mut idx: i64 = (level0.content.len() as i64) - 2;

            while idx >= 0 {
                let cursor = level0.content[idx as usize].segments.open_cursor();
                cursor_repo.push(cursor.into());
                idx -= 1;
            }

            MultiCursor::new(cursor_repo)
        };

        let segment = self.merge_level(snapshot, cursor, preserve_delete)?;

        Ok(segment)
    }

    fn merge_level(&mut self, snapshot: &mut LsmSnapshot, cursor: MultiCursor, preserve_delete: bool) -> DbResult<ImLsmSegment> {
        let result = lsm_backend_utils::merge_level(cursor, preserve_delete)?;
        self.write_merged_tuples(snapshot, &result.tuples, result.estimate_size)
    }

    fn estimate_mem_table_byte_size(mem_table: &MemTable) -> usize {
        let mut result: usize = 0;

        let mut cursor = mem_table.open_cursor();
        cursor.go_to_min();

        while !cursor.done() {
            let tuple = cursor.tuple();
            if let Some((key, value)) = &tuple {
                result += lsm_backend_utils::estimate_key_size(key);

                match value {
                    LsmTreeValueMarker::Value(v) => {

                        result += vli::vli_len_u64(v.len() as u64);

                        result += v.len();
                    }
                    _ => ()
                }
            }

            cursor.next();
        }

        result
    }

    fn get_start_writing_pid(&self, snapshot: &mut LsmSnapshot, estimate_size: usize) -> (u64, Option<FreeSegmentRecord>) {
        let page_size = self.config.get_lsm_page_size();

        let mut index: usize = 0;
        for seg in &snapshot.free_segments {
            let page_count = seg.end_pid - seg.start_pid + 1;
            let seg_size = (page_count as usize) * (page_size as usize);

            if seg_size > estimate_size {
                return self.choose_selected_segments(snapshot, index, estimate_size);
            }

            index += 1;
        }

        (snapshot.file_size / (page_size as u64), None)
    }

    fn choose_selected_segments(&self, snapshot: &mut LsmSnapshot, index: usize, _estimate_size: usize) -> (u64, Option<FreeSegmentRecord>) {
        let seg = snapshot.free_segments[index];
        let start_pid = seg.start_pid;

        snapshot.free_segments.remove(index);

        self.metrics.add_use_free_segment_count();

        (start_pid, Some(seg))
    }

    fn write_merged_tuples(
        &mut self,
        snapshot: &mut LsmSnapshot,
        tuples: &[(Arc<[u8]>, LsmTreeValueMarker<LsmTuplePtr>)],
        estimate_size: usize,
    ) -> DbResult<ImLsmSegment> {
        let config = self.config.clone();
        let page_size = config.get_lsm_page_size();

        let mmap = unsafe{
            Mmap::map(&self.file)?
        };

        let (start_pid, used_free_segment) = self.get_start_writing_pid(snapshot, estimate_size);

        let mut writer = FileWriter::open(
            &mut self.file,
            start_pid,
            config,
        );

        writer.begin()?;

        let mut segments = LsmTree::<Arc<[u8]>, LsmTuplePtr>::new();

        for (key, value) in tuples {
           let tuple =  match value {
                LsmTreeValueMarker::Deleted => {
                    writer.write_tuple(key, LsmTreeValueMarker::Deleted)?
                },
                LsmTreeValueMarker::DeleteStart => {
                    writer.write_tuple(key, LsmTreeValueMarker::DeleteStart)?
                },
                LsmTreeValueMarker::DeleteEnd => {
                    writer.write_tuple(key, LsmTreeValueMarker::DeleteEnd)?
                },
                LsmTreeValueMarker::Value(legacy_tuple) => {
                    let offset = ((legacy_tuple.pid as usize) * (page_size as usize)) + (legacy_tuple.offset as usize);
                    let tuple_ptr = writer.write_buffer(&mmap[offset..(offset + (legacy_tuple.byte_size as usize))])?;
                    LsmTreeValueMarker::Value(tuple_ptr)
                }
            };
            segments.update_in_place(key.clone(), tuple);
        }

        let end_ptr = writer.end()?;

        let im_seg = ImLsmSegment {
            segments,
            start_pid,
            end_pid: end_ptr.pid,
        };

        LsmFileBackendInner::return_used_segment(used_free_segment.as_ref(), end_ptr.pid, snapshot);

        Ok(im_seg)
    }

    fn checkpoint_snapshot(&mut self, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let meta_pid = snapshot.meta_pid as u64;
        let next_meta_pid = snapshot.next_meta_pid();
        let mut meta_page = self.read_page(meta_pid)?;

        snapshot.write_to_page(&mut meta_page);

        // update pid and write page
        meta_page.page_id = next_meta_pid as u32;
        self.write_page(&meta_page)?;

        // update snapshot after write page successfully
        snapshot.meta_id += 1;
        snapshot.meta_pid = next_meta_pid;
        Ok(())
    }

    fn read_page(&mut self, pid: u64) -> DbResult<RawPage> {
        let page_size = self.config.get_lsm_page_size();
        let offset = (page_size as u64) * pid;

        let mut result = RawPage::new(pid as u32, NonZeroU32::new(page_size).unwrap());
        result.read_from_file(&mut self.file, offset)?;

        Ok(result)
    }

    fn write_page(&mut self, page: &RawPage) -> DbResult<()> {
        let page_size = self.config.get_lsm_page_size();
        let offset = (page_size as u64) * (page.page_id as u64);
        page.sync_to_file(&mut self.file, offset)?;
        Ok(())
    }
}
