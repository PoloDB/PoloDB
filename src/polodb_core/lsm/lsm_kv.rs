/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use crate::{Config, DbErr, DbResult};
use super::lsm_snapshot::LsmSnapshot;
use super::lsm_backend::{LsmFileBackend, LsmLog};
use super::lsm_segment::LsmSegment;

pub(crate) struct LsmKv {
    backend: Box<LsmFileBackend>,
    log: Option<LsmLog>,
    snapshot: LsmSnapshot,
    draft: RefCell<Option<LsmSegment>>,
    config: Config,
}

impl LsmKv {

    fn mk_log_path(db_path: &Path) -> PathBuf {
        let mut buf = db_path.to_path_buf();
        let filename = buf.file_name().unwrap().to_str().unwrap();
        let new_filename = String::from(filename) + ".wal";
        buf.set_file_name(new_filename);
        buf
    }

    #[allow(dead_code)]
    pub fn open_file(path: &Path, config: Config) -> DbResult<LsmKv> {
        let backend = LsmFileBackend::open(path, config.clone())?;

        let log_file = LsmKv::mk_log_path(path);
        let log = LsmLog::open(log_file.as_path(), config.clone())?;

        let snapshot = LsmSnapshot::new();
        Ok(LsmKv {
            backend: Box::new(backend),
            log: Some(log),
            snapshot,
            draft: RefCell::new(None),
            config,
        })
    }

    pub fn start_transaction(&self) -> DbResult<()> {
        if let Some(log) = &self.log {
            log.start_transaction()?;
        }

        {
            let segment = LsmSegment::new(self.snapshot.segment_pid);
            let mut draft = self.draft.borrow_mut();
            *draft = Some(segment);
        }

        Ok(())
    }

    pub fn rollback(&self) -> DbResult<()> {
        if let Some(log) = &self.log {
            log.rollback()?;
        }

        let mut segment = self.draft.borrow_mut();
        *segment = None;

        Ok(())
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> DbResult<()> {
        if let Some(log) = &self.log {
            log.put(key, value)?;
        }

        let mut segment = self.draft.borrow_mut();
        if segment.is_none() {
            return Err(DbErr::NoTransactionStarted);
        }

        segment.as_mut().unwrap().put(key, value);

        Ok(())
    }

    pub fn commit(&self) -> DbResult<()> {
        if let Some(log) = &self.log {
            log.commit()?;
        }

        let mut segment = self.draft.borrow_mut();
        if segment.is_none() {
            return Err(DbErr::NoTransactionStarted);
        }
        let store_bytes = segment.as_ref().unwrap().store_bytes();
        let block_size = self.config.get_lsm_block_size();
        if store_bytes > (block_size / 2) as usize {
            self.backend.sync_latest_segment(segment.as_ref().unwrap())?;
        }

        *segment = None;

        Ok(())
    }

}
