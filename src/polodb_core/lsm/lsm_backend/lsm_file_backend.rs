/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Mutex;
use memmap2::Mmap;
use crate::{Config, DbErr, DbResult};
use crate::lsm::lsm_backend::file_writer::FileWriter;
use crate::lsm::mem_table::MemTable;
use crate::lsm::lsm_segment::{ImLsmSegment, LsmTuplePtr};
use crate::lsm::lsm_snapshot::{LsmMetaDelegate, LsmSnapshot};
use crate::page::RawPage;
use crate::lsm::lsm_tree::LsmTree;
use crate::lsm::lsm_snapshot::lsm_meta::{
    DB_FILE_SIZE_OFFSET,
    LOG_OFFSET_OFFSET,
    META_ID_OFFSET,
};

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
    use super::file_lock::exclusive_lock_file;
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
        config: Config,
    ) -> DbResult<LsmFileBackend> {
        let inner = LsmFileBackendInner::open(path, config)?;
        Ok(LsmFileBackend {
            inner: Mutex::new(inner),
        })
    }

    pub fn read_latest_snapshot(&self) -> DbResult<LsmSnapshot> {
        let mut inner = self.inner.lock()?;
        inner.read_latest_snapshot()
    }

    pub fn sync_latest_segment(&self, segment: &MemTable, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        inner.sync_latest_segment(segment, snapshot)
    }

    pub fn checkpoint_snapshot(&self, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        inner.checkpoint_snapshot(snapshot)
    }
}

struct LsmFileBackendInner {
    file:    File,
    config:  Config,
}

impl LsmFileBackendInner {

    fn open(
        path: &Path,
        config: Config,
    ) -> DbResult<LsmFileBackendInner> {
        let file = open_file_native(path)?;
        Ok(LsmFileBackendInner {
            file,
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

    fn read_latest_snapshot(&mut self) -> DbResult<LsmSnapshot> {
        let meta = self.file.metadata()?;
        if meta.len() == 0 { // new file
            return self.force_init_file();
        }

        let mmap = unsafe {
            Mmap::map(&self.file)?
        };

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

        if meta1 > meta2 {
            LsmFileBackendInner::read_snapshot_from_page(
                &mmap[0..(page_size as usize)],
                0,
                meta1,
            )
        } else {
            let start = page_size as usize;
            LsmFileBackendInner::read_snapshot_from_page(
                &mmap[start..(start + page_size as usize)],
                1,
                meta2,
            )
        }
    }

    fn read_snapshot_from_page(slice: &[u8], meta_pid: u8, meta_id: u64) -> DbResult<LsmSnapshot> {
        let mut db_file_size_be: [u8; 8] = [0; 8];
        db_file_size_be.copy_from_slice(&slice[(DB_FILE_SIZE_OFFSET as usize)..((DB_FILE_SIZE_OFFSET + 8) as usize)]);
        let db_file_size = u64::from_be_bytes(db_file_size_be);

        let mut log_offset_be: [u8; 8] = [0; 8];
        log_offset_be.copy_from_slice(&slice[(LOG_OFFSET_OFFSET as usize)..((LOG_OFFSET_OFFSET + 8) as usize)]);
        let log_offset = u64::from_be_bytes(log_offset_be);

        let result = LsmSnapshot {
            meta_pid,
            meta_id,
            file_size: db_file_size,
            log_offset,
            free_segments: Vec::with_capacity(4),
            levels: Vec::with_capacity(4),
        };

        Ok(result)
    }

    fn sync_latest_segment(&mut self, mem_table: &MemTable, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let config = self.config.clone();
        let page_size = config.get_lsm_page_size();

        // TODO: try alloc from free pages
        let start_pid = snapshot.file_size / (page_size as u64);

        let mut writer = FileWriter::open(
            &mut self.file,
            start_pid,
            snapshot,
            config,
        );

        writer.begin()?;

        let mut segments = LsmTree::<Box<[u8]>, LsmTuplePtr>::new();

        let mut mem_table_cursor = mem_table.segments.open_cursor();

        while !mem_table_cursor.done() {
            let (key, value) = mem_table_cursor.tuple().unwrap();
            let pos = writer.write_tuple(key.as_ref(), value.as_ref())?;

            segments.insert_in_place(key, pos);

            mem_table_cursor.next();
        }

        let end_ptr = writer.end()?;

        let im_seg = ImLsmSegment {
            segments,
            start_pid,
            end_pid: end_ptr.pid,
        };

        snapshot.add_latest_segment(im_seg);
        snapshot.file_size = self.file.seek(SeekFrom::End(0))?;

        Ok(())
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
