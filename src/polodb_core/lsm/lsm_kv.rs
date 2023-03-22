/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::cell::{Cell, RefCell};
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

    pub fn open_file(path: &Path) -> DbResult<LsmKv> {
        let config = Config::default();
        LsmKv::open_file_with_config(path, config)
    }

    pub fn open_file_with_config(path: &Path, config: Config) -> DbResult<LsmKv> {
        let inner = LsmKvInner::open_file(path, config)?;
        LsmKv::open_with_inner(inner)
    }

    pub fn open_memory() -> DbResult<LsmKv> {
        LsmKv::open_memory_with_config(Config::default())
    }

    pub fn open_memory_with_config(config: Config) -> DbResult<LsmKv> {
        let inner = LsmKvInner::open_with_backend(None, None, config)?;
        LsmKv::open_with_inner(inner)
    }

    #[inline]
    fn open_with_inner(inner: LsmKvInner) -> DbResult<LsmKv> {
        Ok(LsmKv {
            inner: Arc::new(inner),
        })
    }

    pub fn open_cursor(&self) -> KvCursor {
        let multi_cursor = self.inner.open_multi_cursor();
        KvCursor::new(self.inner.clone(), multi_cursor)
    }

    pub fn put<K, V>(&self, key: K, value: V) -> DbResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.inner.start_transaction()?;
        self.inner.put(key.as_ref(), value.as_ref())?;
        self.inner.commit()
    }

    pub fn delete<K>(&self, key: K) -> DbResult<()>
    where
        K: AsRef<[u8]>
    {
        self.inner.start_transaction()?;
        self.inner.delete(key.as_ref())?;
        self.inner.commit()
    }

    pub fn get<'a, K>(&self, key: K) -> DbResult<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let cursor = self.open_cursor();
        cursor.seek(key.as_ref())?;
        let value = cursor.value()?;
        let result = match value {
            Some(bytes) => Some(bytes),
            None => None,
        };
        Ok(result)
    }

    pub fn get_string<'a, K>(&self, key: K) -> DbResult<Option<String>>
        where
            K: AsRef<[u8]>,
    {
        let bytes = self.get(key)?;
        let string = match bytes {
            None => None,
            Some(bytes) => {
                let result = String::from_utf8(bytes)?;
                Some(result)
            }
        };
        Ok(string)
    }

}

pub(crate) struct LsmKvInner {
    backend: Option<Box<LsmFileBackend>>,
    log: Option<LsmLog>,
    snapshot: Mutex<LsmSnapshot>,
    mem_table: RefCell<MemTable>,
    in_transaction: Cell<bool>,
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
        LsmKvInner::open_with_backend(Some(Box::new(backend)), Some(log), config)
    }

    fn open_with_backend(
        backend: Option<Box<LsmFileBackend>>,
        log: Option<LsmLog>,
        config: Config,
    ) -> DbResult<LsmKvInner> {
        let snapshot = match &backend {
            Some(backend) => backend.read_latest_snapshot()?,
            None => LsmSnapshot::new(),
        };
        let mut mem_table = MemTable::new(0);

        if let Some(log) = &log {
            log.update_mem_table_with_latest_log(
                &snapshot,
                &mut mem_table,
            )?;
        }

        Ok(LsmKvInner {
            backend,
            log,
            snapshot: Mutex::new(snapshot),
            mem_table: RefCell::new(mem_table),
            in_transaction: Cell::new(false),
            config,
        })
    }

    fn open_multi_cursor(&self) -> MultiCursor {
        let mem_table = self.mem_table.borrow();
        let mem_table_cursor = mem_table.segments.open_cursor();
        MultiCursor::new(mem_table_cursor)
    }

    fn start_transaction(&self) -> DbResult<()> {
        if let Some(log) = &self.log {
            log.start_transaction()?;
        }

        self.in_transaction.set(true);

        Ok(())
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> DbResult<()> {
        if !self.in_transaction.get() {
            return Err(DbErr::NoTransactionStarted);
        }

        if let Some(log) = &self.log {
            log.put(key, value)?;
        }

        let mut segment = self.mem_table.borrow_mut();

        segment.put(key, value);

        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> DbResult<()> {
        if !self.in_transaction.get() {
            return Err(DbErr::NoTransactionStarted);
        }

        if let Some(log) = &self.log {
            log.delete(key)?;
        }

        let mut segment = self.mem_table.borrow_mut();

        segment.delete(key);

        Ok(())
    }

    fn commit(&self) -> DbResult<()> {
        if !self.in_transaction.get() {
            return Err(DbErr::NoTransactionStarted);
        }

        if let Some(log) = &self.log {
            let commit_result = log.commit()?;
            let mut snapshot = self.snapshot.lock()?;
            snapshot.log_offset = commit_result.offset;
        }

        if let Some(backend) = &self.backend {
            let mut mem_table = self.mem_table.borrow_mut();

            let store_bytes = mem_table.store_bytes();
            let block_size = self.config.get_lsm_block_size();
            if store_bytes > (block_size / 2) as usize {
                let mut snapshot = self.snapshot.lock()?;
                backend.sync_latest_segment(
                    &mut mem_table,
                    &mut snapshot,
                )?;
                backend.checkpoint_snapshot(&mut snapshot)?;

                mem_table.segments.clear();
            }
        }

        self.in_transaction.set(false);

        Ok(())
    }

    pub(crate) fn meta_id(&self) -> u64 {
        let snapshot = self.snapshot.lock().unwrap();
        snapshot.meta_id
    }

}
