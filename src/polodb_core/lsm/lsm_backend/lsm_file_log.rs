/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use crc64fast::Digest;
use getrandom::getrandom;
use memmap2::Mmap;
use crate::{Config, ErrorKind, Result};
use crate::lsm::lsm_backend::lsm_log::{LsmLog, format, LsmCommitResult, lsm_log_utils};
use crate::lsm::lsm_snapshot::LsmSnapshot;
use crate::lsm::mem_table::MemTable;

static HEADER_DESP: &str       = "PoloDB Journal v0.4";
const DATABASE_VERSION: [u8; 4] = [0, 0, 4, 0];
const DATA_BEGIN_OFFSET: u64 = 64;

struct LogTransactionState {
    digest: Digest,
}

impl LogTransactionState {

    fn new() -> LogTransactionState {
        LogTransactionState {
            digest: Digest::new(),
        }
    }

}

pub(crate) struct LsmFileLog {
    inner: Mutex<LsmFileLogInner>
}

impl LsmFileLog {

    pub fn open(path: &Path, config: Arc<Config>) -> Result<LsmFileLog> {
        let inner = LsmFileLogInner::open(path, config)?;
        Ok(LsmFileLog {
            inner: Mutex::new(inner),
        })
    }

    #[allow(dead_code)]
    pub fn path(&self) -> PathBuf {
        let inner = self.inner.lock().unwrap();
        inner.file_path.to_path_buf()
    }

}

impl LsmLog for LsmFileLog {

    fn start_transaction(&self) -> Result<()> {
        let mut inner = self.inner.lock()?;
        inner.start_transaction()
    }

    fn commit(&self, buffer: Option<&[u8]>) -> Result<LsmCommitResult> {
        let mut inner = self.inner.lock()?;
        inner.commit(buffer)
    }

    fn update_mem_table_with_latest_log(
        &self,
        snapshot: &LsmSnapshot,
        mem_table: &mut MemTable,
    ) -> Result<()> {
        let mut inner = self.inner.lock()?;
        inner.update_mem_table_with_latest_log(snapshot, mem_table)
    }

    fn shrink(&self, snapshot: &mut LsmSnapshot) -> Result<()> {
        let mut inner = self.inner.lock()?;
        inner.shrink(snapshot)
    }

    fn enable_safe_clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.safe_clear = true;
    }

}

fn generate_a_salt() -> u32 {
    let mut buf: [u8; 4] = [0; 4];
    getrandom(&mut buf).unwrap();
    u32::from_le_bytes(buf)
}

fn crc64(bytes: &[u8]) -> u64 {
    let mut c = Digest::new();
    c.write(bytes);
    c.sum64()
}

struct LsmFileLogInner {
    #[allow(dead_code)]
    file_path:   PathBuf,
    file:        File,
    transaction: Option<LogTransactionState>,
    offset:      u64,
    salt1:       u32,
    salt2:       u32,
    config:      Arc<Config>,
    safe_clear:  bool,
}

impl LsmFileLogInner {

    fn open(path: &Path, config: Arc<Config>) -> Result<LsmFileLogInner> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
        let meta = file.metadata()?;

        let file_path: PathBuf = path.to_path_buf();
        let mut result = LsmFileLogInner {
            file_path,
            file,
            transaction: None,
            offset: 0,
            salt1: generate_a_salt(),
            salt2: generate_a_salt(),
            config,
            safe_clear: false,
        };

        if meta.len() == 0 {
            result.force_init_header()?;
        } else {
            result.read_and_check_from_file()?;
        }

        Ok(result)
    }

    fn shrink(&mut self, snapshot: &mut LsmSnapshot) -> Result<()> {
        self.file.set_len(DATA_BEGIN_OFFSET)?;
        self.offset = self.file.seek(SeekFrom::End(0))?;

        snapshot.log_offset = 0;

        Ok(())
    }

    /// name:       32 bytes
    /// version:    4bytes(offset 32)
    /// page_size:  4bytes(offset 36)
    /// salt_1:     4bytes(offset 40)
    /// salt_2:     4bytes(offset 44)
    /// checksum before 48:   8bytes(offset 48)
    /// data begin: 64 bytes
    fn force_init_header(&mut self) -> Result<()> {
        let mut header48: Vec<u8> = vec![];
        header48.resize(48, 0);

        // copy title
        let title_bytes = HEADER_DESP.as_bytes();
        header48[0..title_bytes.len()].copy_from_slice(title_bytes);

        // copy version
        header48[32..36].copy_from_slice(&DATABASE_VERSION);
        let page_size_be = self.config.lsm_page_size.to_be_bytes();
        header48[36..40].copy_from_slice(&page_size_be);

        let salt_1_be = self.salt1.to_be_bytes();
        header48[40..44].copy_from_slice(&salt_1_be);

        let salt_2_be = self.salt2.to_be_bytes();
        header48[44..48].copy_from_slice(&salt_2_be);

        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header48)?;

        let checksum = crc64(&header48);
        let checksum_be = checksum.to_be_bytes();

        self.file.seek(SeekFrom::Start(48))?;
        self.file.write_all(&checksum_be)?;

        self.file.flush()?;

        self.file.set_len(DATA_BEGIN_OFFSET)?;
        self.offset = self.file.seek(SeekFrom::End(0))?;

        Ok(())
    }

    fn read_and_check_from_file(&mut self) -> Result<()> {
        let mut header48: Vec<u8> = vec![0; 48];
        self.file.read_exact(&mut header48)?;

        let checksum = crc64(&header48);
        let checksum_from_file = self.read_checksum_from_file()?;
        if checksum != checksum_from_file {
            return Err(ErrorKind::ChecksumMismatch.into());
        }

        // // copy version
        // self.version.copy_from_slice(&header48[32..36]);

        let mut buffer: [u8; 4] = [0; 4];
        buffer.copy_from_slice(&header48[40..44]);
        self.salt1 = u32::from_be_bytes(buffer);

        let mut buffer: [u8; 4] = [0; 4];
        buffer.copy_from_slice(&header48[44..48]);
        self.salt2 = u32::from_be_bytes(buffer);

        self.offset = DATA_BEGIN_OFFSET;
        self.file.seek(SeekFrom::Start(DATA_BEGIN_OFFSET))?;

        Ok(())
    }

    pub fn update_mem_table_with_latest_log(&mut self, snapshot: &LsmSnapshot, mem_table: &mut MemTable) -> Result<()> {
        let start_offset = (snapshot.log_offset + DATA_BEGIN_OFFSET) as usize;

        let (start_offset, reset) = {
            let mmap = unsafe {
                Mmap::map(&self.file)?
            };

            lsm_log_utils::update_mem_table_by_buffer(&mmap, start_offset, mem_table, false)
        };

        if reset {
            self.file.set_len(start_offset as u64)?;
            self.offset = self.file.seek(SeekFrom::End(0))?;
        }

        Ok(())
    }

    fn read_checksum_from_file(&mut self) -> Result<u64> {
        self.file.seek(SeekFrom::Start(48))?;
        let mut buffer: [u8; 8] = [0; 8];
        self.file.read_exact(&mut buffer)?;
        Ok(u64::from_be_bytes(buffer))
    }

    fn write_without_checksum(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.file.write(bytes)?;
        self.offset += bytes.len() as u64;
        Ok(())
    }

    /// Go to the end of the file
    pub fn start_transaction(&mut self) -> Result<()> {
        let state = LogTransactionState::new();
        self.transaction = Some(state);
        self.offset = self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    pub fn commit(&mut self, buffer: Option<&[u8]>) -> Result<LsmCommitResult> {
        if self.transaction.is_none() {
            return Err(ErrorKind::NoTransactionStarted.into());
        }

        if let Some(buffer) = buffer {
            self.write_all(buffer)?;
        }

        {
            let state = self.transaction.as_ref().unwrap();
            let checksum = state.digest.sum64();
            let checksum_be: [u8; 8] = checksum.to_be_bytes();

            self.write_without_checksum(&[format::COMMIT])?;
            self.write_without_checksum(&checksum_be)?;
        }
        self.transaction = None;
        self.file.flush()?;
        self.offset = self.file.seek(SeekFrom::End(0))?;

        Ok(LsmCommitResult {
            offset: self.offset - DATA_BEGIN_OFFSET,
        })
    }

}

impl Write for LsmFileLogInner {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.transaction.is_none() {
            return Err(std::io::ErrorKind::NotFound.into());
        }
        let state = self.transaction.as_mut().unwrap();
        state.digest.write(buf);

        self.write_without_checksum(buf)?;

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for LsmFileLogInner {

    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.file_path);
    }

}
