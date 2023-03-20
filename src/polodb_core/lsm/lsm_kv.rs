/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use crate::{Config, DbErr, DbResult};
use crate::lsm::kv_cursor::KvCursor;
use super::lsm_snapshot::LsmSnapshot;
use super::lsm_backend::{LsmFileBackend, LsmLog};
use crate::lsm::mem_table::MemTable;
use crate::lsm::multi_cursor::MultiCursor;

#[derive(Clone)]
pub struct LsmKv {
    inner: Arc<LsmKvInner>,
}

impl LsmKv {

    pub fn open_file(path: &Path, config: Config) -> DbResult<LsmKv> {
        let inner = LsmKvInner::open_file(path, config)?;
        Ok(LsmKv {
            inner: Arc::new(inner),
        })
    }

    pub fn open_cursor(&self) -> KvCursor {
        let multi_cursor = self.inner.open_multi_cursor();
        KvCursor::new(self.inner.clone(), multi_cursor)
    }

    pub fn start_transaction(&self) -> DbResult<()> {
        self.inner.start_transaction()
    }

    pub fn rollback(&self) -> DbResult<()> {
        self.inner.rollback()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> DbResult<()> {
        self.inner.put(key, value)
    }

    pub fn commit(&self) -> DbResult<()> {
        self.inner.commit()
    }

}

pub(crate) struct LsmKvInner {
    backend: Box<LsmFileBackend>,
    log: Option<LsmLog>,
    snapshot: Mutex<LsmSnapshot>,
    mem_table: RefCell<Option<MemTable>>,
    config: Config,
}

impl LsmKvInner {

    fn mk_log_path(db_path: &Path) -> PathBuf {
        let mut buf = db_path.to_path_buf();
        let filename = buf.file_name().unwrap().to_str().unwrap();
        let new_filename = String::from(filename) + ".wal";
        buf.set_file_name(new_filename);
        buf
    }

    fn open_file(path: &Path, config: Config) -> DbResult<LsmKvInner> {
        let backend = LsmFileBackend::open(path, config.clone())?;

        let log_file = LsmKvInner::mk_log_path(path);
        let log = LsmLog::open(log_file.as_path(), config.clone())?;

        let snapshot = LsmSnapshot::new();
        Ok(LsmKvInner {
            backend: Box::new(backend),
            log: Some(log),
            snapshot: Mutex::new(snapshot),
            mem_table: RefCell::new(None),
            config,
        })
    }

    fn open_multi_cursor(&self) -> MultiCursor {
        let mut mem_table = self.mem_table.borrow_mut();
        let mem_table_cursor = mem_table.as_mut().unwrap().segments.open_cursor();
        MultiCursor::new(mem_table_cursor)
    }

    fn start_transaction(&self) -> DbResult<()> {
        if let Some(log) = &self.log {
            log.start_transaction()?;
        }

        {
            let segment_pid = {
                let snapshot = self.snapshot.lock()?;
                snapshot.segment_pid()
            };
            let segment = MemTable::new(segment_pid);
            let mut draft = self.mem_table.borrow_mut();
            *draft = Some(segment);
        }

        Ok(())
    }

    fn rollback(&self) -> DbResult<()> {
        if let Some(log) = &self.log {
            log.rollback()?;
        }

        let mut segment = self.mem_table.borrow_mut();
        *segment = None;

        Ok(())
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> DbResult<()> {
        if let Some(log) = &self.log {
            log.put(key, value)?;
        }

        let mut segment = self.mem_table.borrow_mut();
        if segment.is_none() {
            return Err(DbErr::NoTransactionStarted);
        }

        segment.as_mut().unwrap().put(key, value);

        Ok(())
    }

    fn commit(&self) -> DbResult<()> {
        if let Some(log) = &self.log {
            let commit_result = log.commit()?;
            let mut snapshot = self.snapshot.lock()?;
            snapshot.log_offset = commit_result.offset;
        }

        let mut mem_table = self.mem_table.borrow_mut();
        if mem_table.is_none() {
            return Err(DbErr::NoTransactionStarted);
        }
        let store_bytes = mem_table.as_ref().unwrap().store_bytes();
        let block_size = self.config.get_lsm_block_size();
        if store_bytes > (block_size / 2) as usize {
            let mut snapshot = self.snapshot.lock()?;
            self.backend.sync_latest_segment(
                mem_table.as_ref().unwrap(),
                &mut snapshot,
            )?;
            self.backend.checkpoint_snapshot(&mut snapshot)?;
        }

        *mem_table = None;

        Ok(())
    }

    pub(crate) fn meta_id(&self) -> u64 {
        let snapshot = self.snapshot.lock().unwrap();
        snapshot.meta_id
    }

}
