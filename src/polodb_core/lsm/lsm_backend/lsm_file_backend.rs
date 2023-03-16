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
use byteorder::WriteBytesExt;
use crate::{Config, DbErr, DbResult};
use crate::lsm::mem_table::MemTable;
use crate::lsm::lsm_segment::{ImLsmSegment, LsmTuplePtr, SegValue};
use crate::lsm::lsm_snapshot::LsmSnapshot;
use crate::page::RawPage;
use crate::lsm::lsm_tree::LsmTree;
use super::lsm_meta::LsmMetaDelegate;

#[allow(unused)]
mod format {
    pub const LSM_START_DELETE: u8 = 0x01;
    pub const LSM_END_DELETE: u8   = 0x02;
    pub const LSM_POINT_DELETE: u8 = 0x03;
    pub const LSM_INSERT: u8       = 0x04;
    pub const LSM_SEPARATOR: u8    = 0x10;
    pub const LSM_SYSTEMKEY: u8    = 0x20;
}

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

    pub fn sync_latest_segment(&self, segment: &MemTable, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        inner.sync_latest_segment(segment, snapshot)
    }

    pub fn checkpoint_snapshot(&self, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        inner.checkpoint_snapshot(snapshot)
    }
}

struct FileWriter<'a, 'b> {
    file:                 &'a mut File,
    pid:                  u64,
    page_count_per_block: u64,
    page_buffer:          RawPage,
    snapshot:             &'b mut LsmSnapshot,
    config:               Config,
}

fn new_page_in_block(pid: u64, std_size: u32, id_in_block: u64, page_count_per_block: u64) -> RawPage {
    let byte_size = if id_in_block == page_count_per_block - 1 || id_in_block == 0 {  // last page of the block
        NonZeroU32::new(std_size - 4).unwrap()
    } else {
        NonZeroU32::new(std_size).unwrap()
    };
    RawPage::new(pid as u32, byte_size)
}

impl<'a, 'b> FileWriter<'a, 'b> {

    fn open(file: &'a mut File, pid: u64, snapshot: &'b mut LsmSnapshot, config: Config) -> FileWriter<'a, 'b> {
        let block_size = config.get_lsm_block_size();
        let page_size = config.get_lsm_page_size();

        let page_count_per_block = (block_size / page_size) as u64;
        let id_in_block = pid % page_count_per_block;

        let page_buffer = new_page_in_block(pid, page_size, id_in_block, page_count_per_block);
        FileWriter {
            file,
            pid,
            page_count_per_block,
            page_buffer,
            snapshot,
            config,
        }
    }

    fn write_tuple(&mut self, key: &[u8], value: &SegValue) -> DbResult<LsmTuplePtr> {
        let pos = LsmTuplePtr {
            pid: self.pid,
            offset: self.page_buffer.pos(),
        };
        match value {
            SegValue::OwnValue(insert_buffer) => {
                self.write_u8(format::LSM_INSERT)?;
                crate::btree::vli::encode(self, key.len() as i64)?;
                self.write_all(key)?;

                let value_len = insert_buffer.len();
                crate::btree::vli::encode(self, value_len as i64)?;
                self.write_all(&insert_buffer)?;
            }
            SegValue::Deleted => {
                self.write_u8(format::LSM_POINT_DELETE)?;
                crate::btree::vli::encode(self, key.len() as i64)?;
                self.write_all(key)?;
            }
        }
        Ok(pos)
    }

    fn begin(&mut self) -> DbResult<()> {
        let page_size = self.config.get_lsm_page_size();
        let offset = (page_size as u64) * self.pid;

        self.file.seek(SeekFrom::Start(offset))?;

        Ok(())
    }

    fn end(&mut self) -> DbResult<LsmTuplePtr> {
        Ok(LsmTuplePtr {
            pid: self.pid,
            offset: self.page_buffer.pos(),
        })
    }

    fn enlarge_database_file(&mut self) -> std::io::Result<u64> {
        let file_meta = self.file.metadata()?;
        let current_size = file_meta.len();
        let page_size = self.config.get_lsm_page_size();
        let block_size = self.config.get_lsm_block_size();
        let new_size = current_size + (block_size as u64);
        self.file.set_len(new_size)?;

        let new_page_id = current_size / (page_size as u64);
        Ok(new_page_id)
    }

    /// 1. Write current page to the file
    /// 2. Make the writer point to the new page
    fn next_page(&mut self) -> std::io::Result<()> {
        let id_in_block = self.pid % self.page_count_per_block;
        let page_size = self.config.get_lsm_page_size();
        if id_in_block == self.page_count_per_block - 1 {  // last page int the block
            // write all data to the current page
            self.file.write_all(&self.page_buffer.data)?;

            let new_page_id = if self.snapshot.free_blocks.is_empty() {
                self.enlarge_database_file()?
            } else {
                let block_id = self.snapshot.consume_free_blocks();
                (block_id as u64) * self.page_count_per_block
            };

            let new_page_id_be = new_page_id.to_be_bytes();
            self.file.write_all(&new_page_id_be)?;

            self.pid = new_page_id;
            let id_in_block = self.pid % self.page_count_per_block;
            self.page_buffer = new_page_in_block(
                self.pid,
                page_size,
                id_in_block,
                self.page_count_per_block,
            );
        } else {
            if id_in_block == 0 {
                // preserve 4 bytes for prev pointer
                self.file.seek(SeekFrom::Current(4))?;
            }
            self.file.write_all(&self.page_buffer.data)?;

            self.pid += 1;
            let id_in_block = self.pid % self.page_count_per_block;
            self.page_buffer = new_page_in_block(
                self.pid,
                page_size,
                id_in_block,
                self.page_count_per_block,
            );
        }
        Ok(())
    }
}

impl Write for FileWriter<'_, '_> {

    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let remain_size = self.page_buffer.remain_size() as usize;
        let buf_size = buf.len();
        if remain_size >= buf_size {
            self.page_buffer.put(buf);
            return Ok(buf.len())
        }

        self.page_buffer.put(&buf[0..remain_size]);

        self.next_page()?;

        Ok(remain_size)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
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

        let mut result = LsmFileBackendInner {
            file,
            config,
        };

        result.init_db()?;

        Ok(result)
    }

    fn init_db(&mut self) -> DbResult<()> {
        let file_meta = self.file.metadata()?;
        if file_meta.len() == 0 {
            self.force_init_file()?;
        }

        Ok(())
    }

    fn force_init_file(&mut self) -> DbResult<()> {
        let delegate = LsmMetaDelegate::new(
            self.config.get_lsm_page_size(),
            self.config.get_lsm_block_size(),
        );
        delegate.0.sync_to_file(&mut self.file, 0)?;
        self.file.flush()?;

        let block_size = self.config.get_lsm_block_size() as u64;
        let page_size = self.config.get_lsm_page_size();
        let meta_size = (page_size * 2) as u64;
        self.file.set_len(block_size)?;
        self.file.seek(SeekFrom::Start(meta_size))?;

        Ok(())
    }

    fn sync_latest_segment(&mut self, mem_table: &MemTable, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let config = self.config.clone();
        let start_pid = snapshot.pid_ptr;

        let mut writer = FileWriter::open(
            &mut self.file,
            start_pid,
            snapshot,
            config,
        );

        writer.begin()?;

        let mut segments = LsmTree::<Vec<u8>, LsmTuplePtr>::new();

        let mut mem_table_cursor = mem_table.segments.open_cursor();

        while !mem_table_cursor.done() {
            let key = mem_table_cursor.key();
            let value = mem_table_cursor.value();
            let pos = writer.write_tuple(&key, value.as_ref().unwrap())?;

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
        snapshot.pid_ptr = end_ptr.pid + 1;

        Ok(())
    }

    fn checkpoint_snapshot(&mut self, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let meta_pid = snapshot.meta_pid as u64;
        let next_meta_pid = snapshot.next_meta_pid();
        let meta_page = self.read_page(meta_pid)?;

        let mut delegate = LsmMetaDelegate(meta_page);
        delegate.set_meta_id(snapshot.meta_id);
        delegate.set_log_offset(snapshot.log_offset);

        assert!(snapshot.levels.len() < u8::MAX as usize);
        delegate.set_level_count(snapshot.levels.len() as u8);

        delegate.begin_write_level();
        for level in &snapshot.levels {
            delegate.write_level(level);
        }

        // update pid and write page
        delegate.0.page_id = next_meta_pid as u32;
        self.write_page(&delegate.0)?;

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
