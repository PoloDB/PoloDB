/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::io::{Seek, SeekFrom, Write};
use std::fs::File;
use byteorder::WriteBytesExt;
use crate::{Config, DbResult};
use crate::lsm::lsm_segment::LsmTuplePtr;
use crate::lsm::lsm_tree::LsmTreeValueMarker;
use super::format;

/// Write the data to file.
/// Record the position of tuple,
/// to make a index in snapshot.
pub(crate) struct FileWriter<'a> {
    file:          &'a mut File,
    start_pid:     u64,
    page_size:     u32,
    written_bytes: u64,
    config:        Config,
}

impl<'a> FileWriter<'a> {

    pub fn open(file: &'a mut File, start_pid: u64, config: Config) -> FileWriter<'a> {
        let page_size = config.get_lsm_page_size();

        FileWriter {
            file,
            start_pid,
            page_size,
            written_bytes: 0,
            config,
        }
    }

    #[inline]
    pub fn written_bytes(&self) -> u64 {
        self.written_bytes
    }

    fn start_mark(&self) -> LsmTuplePtr {
        let page_id = self.written_bytes / (self.page_size as u64);
        let page_offset = self.written_bytes % (self.page_size as u64);
        LsmTuplePtr {
            pid: self.start_pid + page_id,
            offset: page_offset as u32,
            byte_size: self.written_bytes,
        }
    }

    fn end_mark(&self, start_mark: &LsmTuplePtr) -> LsmTuplePtr {
        LsmTuplePtr {
            pid: start_mark.pid,
            offset: start_mark.offset,
            byte_size: self.written_bytes - start_mark.byte_size,
        }
    }

    pub fn write_tuple(&mut self, key: &[u8], value: LsmTreeValueMarker<&[u8]>) -> DbResult<LsmTuplePtr> {
        let start_mark = self.start_mark();
        match value {
            LsmTreeValueMarker::Value(insert_buffer) => {
                self.write_u8(format::LSM_INSERT)?;
                crate::btree::vli::encode(self, key.len() as i64)?;
                self.write_all(key)?;

                let value_len = insert_buffer.len();
                crate::btree::vli::encode(self, value_len as i64)?;
                self.write_all(&insert_buffer)?;
            }
            LsmTreeValueMarker::Deleted => {
                self.write_u8(format::LSM_POINT_DELETE)?;
                crate::btree::vli::encode(self, key.len() as i64)?;
                self.write_all(key)?;
            }
            LsmTreeValueMarker::DeleteStart => {
                self.write_u8(format::LSM_START_DELETE)?;
                crate::btree::vli::encode(self, key.len() as i64)?;
                self.write_all(key)?;
            }
            LsmTreeValueMarker::DeleteEnd => {
                self.write_u8(format::LSM_END_DELETE)?;
                crate::btree::vli::encode(self, key.len() as i64)?;
                self.write_all(key)?;
            }
        }
        Ok(self.end_mark(&start_mark))
    }

    pub fn write_buffer(&mut self, buffer: &[u8]) -> DbResult<LsmTuplePtr> {
        let start_mark = self.start_mark();

        self.write_all(buffer)?;

        Ok(self.end_mark(&start_mark))
    }

    pub fn begin(&mut self) -> DbResult<()> {
        let page_size = self.config.get_lsm_page_size();
        let offset = (page_size as u64) * self.start_pid;

        self.file.seek(SeekFrom::Start(offset))?;

        Ok(())
    }

    /// write padding to align page
    pub fn end(&mut self) -> DbResult<LsmTuplePtr> {
        let start_mark = self.start_mark();
        let remain_to_next_page = self.page_size - start_mark.offset;

        let white = vec![0; remain_to_next_page as usize];
        self.file.write(&white)?;

        Ok(self.end_mark(&start_mark))
    }

}

impl Write for FileWriter<'_> {

    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written_bytes = self.file.write(buf)?;

        self.written_bytes += written_bytes as u64;

        Ok(written_bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
