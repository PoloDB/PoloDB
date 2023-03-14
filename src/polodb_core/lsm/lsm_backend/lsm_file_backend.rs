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
use crate::lsm::lsm_segment::{LsmSegment, SegValue};
use crate::page::RawPage;
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

    pub fn sync_latest_segment(&self, segment: &LsmSegment) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        inner.sync_latest_segment(segment)
    }

}

struct FileWriter<'a> {
    file:                 &'a mut File,
    pid:                  u64,
    page_count_per_block: u64,
    page_buffer:          RawPage,
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

impl<'a> FileWriter<'a> {

    fn open(file: &'a mut File, pid: u64, config: Config) -> FileWriter {
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
            config,
        }
    }

    fn write_tuple(&mut self, key: &[u8], value: &SegValue) -> DbResult<()> {
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
        Ok(())
    }

    fn begin(&mut self) -> DbResult<()> {
        let page_size = self.config.get_lsm_page_size();
        let offset = (page_size as u64) * self.pid;

        self.file.seek(SeekFrom::Start(offset))?;

        Ok(())
    }

    fn end(&mut self) -> DbResult<()> {
        Ok(())
    }

    fn next_page(&mut self) -> std::io::Result<()> {
        let id_in_block = self.pid & self.page_count_per_block;
        if id_in_block == self.page_count_per_block - 1 {  // last page int the block
            self.file.write_all(&self.page_buffer.data)?;
            unimplemented!()
        } else {
            if id_in_block == 0 {
                // preserve 4 bytes for prev pointer
                self.file.seek(SeekFrom::Current(4))?;
            }
            self.file.write_all(&self.page_buffer.data)?;

            let page_size = self.config.get_lsm_page_size();
            self.pid += 1;
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

impl Write for FileWriter<'_> {

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
    pid_ptr: u64,
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
            pid_ptr: 2,
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

    fn sync_latest_segment(&mut self, segment: &LsmSegment) -> DbResult<()> {
        let config = self.config.clone();
        let mut writer = FileWriter::open(
            &mut self.file,
            self.pid_ptr,
            config,
        );

        writer.begin()?;

        for (key, value) in &segment.segments {
            writer.write_tuple(&key, value)?;
        }

        writer.end()
    }
}
