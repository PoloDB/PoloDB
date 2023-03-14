/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cell::RefCell;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use byteorder::WriteBytesExt;
use crc64fast::Digest;
use crate::{Config, DbErr, DbResult};

static HEADER_DESP: &str       = "PoloDB Journal v0.4";
const DATABASE_VERSION: [u8; 4] = [0, 0, 4, 0];

#[allow(dead_code)]
mod format {
    pub const EOF: u8     = 0x00;
    pub const PAD1: u8    = 0x01;
    pub const PAD2: u8    = 0x02;
    pub const COMMIT: u8  = 0x03;
    pub const JUMP: u8    = 0x04;
    pub const WRITE: u8   = 0x06;
    pub const DELETE: u8  = 0x08;
}

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

pub(crate) struct LsmLog {
    inner: RefCell<LsmLogInner>
}

impl LsmLog {

    pub fn open(path: &Path, config: Config) -> DbResult<LsmLog> {
        let inner = LsmLogInner::open(path, config)?;
        Ok(LsmLog {
            inner: RefCell::new(inner),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> DbResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.put(key, value)
    }

    pub fn start_transaction(&self) -> DbResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.start_transaction()
    }

    pub fn rollback(&self) -> DbResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.rollback()
    }

    pub fn commit(&self) -> DbResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.commit()
    }

}

struct LsmLogInner {
    file_path:   PathBuf,
    file:        File,
    transaction: Option<LogTransactionState>,
    offset:      u64,
    config:      Config,
}

impl LsmLogInner {

    fn open(path: &Path, config: Config) -> DbResult<LsmLogInner> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
        let meta = file.metadata()?;

        let file_path: PathBuf = path.to_path_buf();
        let mut result = LsmLogInner {
            file_path,
            file,
            transaction: None,
            offset: 0,
            config,
        };

        if meta.len() == 0 {
            result.force_init_header()?;
        }

        Ok(result)
    }

    fn force_init_header(&mut self) -> DbResult<()> {
        let mut header48: Vec<u8> = vec![];
        header48.resize(48, 0);

        // copy title
        let title_bytes = HEADER_DESP.as_bytes();
        header48[0..title_bytes.len()].copy_from_slice(title_bytes);

        // copy version
        header48[32..36].copy_from_slice(&DATABASE_VERSION);
        let page_size_be = self.config.get_lsm_page_size().to_be_bytes();
        header48[36..40].copy_from_slice(&page_size_be);

        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header48)?;

        self.file.flush()?;

        self.offset = 48;

        Ok(())
    }

    fn write_without_checksum(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.file.write(bytes)?;
        self.offset += bytes.len() as u64;
        Ok(())
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> DbResult<()> {
        self.write_u8(format::WRITE)?;

        let key_len = key.len();
        crate::btree::vli::encode(self, key_len as i64)?;

        self.write_all(key)?;

        let value_len = value.len();
        crate::btree::vli::encode(self, value_len as i64)?;

        self.write_all(value)?;

        Ok(())
    }

    pub fn start_transaction(&mut self) -> DbResult<()> {
        // TODO: write padding
        let state = LogTransactionState::new();
        self.transaction = Some(state);
        Ok(())
    }

    pub fn rollback(&mut self) -> DbResult<()> {
        self.transaction = None;
        Ok(())
    }

    pub fn commit(&mut self) -> DbResult<()> {
        if self.transaction.is_none() {
            return Err(DbErr::NoTransactionStarted);
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
        Ok(())
    }

}

impl Write for LsmLogInner {
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
