/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::JsValue;
use crate::{Config, DbErr, DbResult, TransactionType};
use crate::lsm::kv_cursor::KvCursor;
use crate::lsm::lsm_backend::LsmBackend;
use crate::lsm::lsm_segment::LsmTuplePtr;
use crate::lsm::lsm_session::LsmSession;
use crate::lsm::LsmMetrics;
use crate::lsm::lsm_snapshot::LsmSnapshot;
use crate::lsm::mem_table::MemTable;
use crate::lsm::multi_cursor::{CursorRepr, MultiCursor};
use crate::transaction::TransactionState;
use super::lsm_backend::LsmLog;
#[cfg(not(target_arch = "wasm32"))]
use super::lsm_backend::{LsmFileBackend, LsmFileLog};
#[cfg(target_arch = "wasm32")]
use super::lsm_backend::{IndexeddbLog, IndexeddbBackend};

#[derive(Clone)]
pub struct LsmKv {
    pub(crate) inner: Arc<LsmKvInner>,
}

impl LsmKv {

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_file(path: &Path) -> DbResult<LsmKv> {
        let config = Config::default();
        LsmKv::open_file_with_config(path, config)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_file_with_config(path: &Path, config: Config) -> DbResult<LsmKv> {
        let inner = LsmKvInner::open_file(path, config)?;
        LsmKv::open_with_inner(inner)
    }


    #[cfg(target_arch = "wasm32")]
    pub fn open_indexeddb(init_data: JsValue) -> DbResult<LsmKv> {
        let config = Arc::new(Config::default());
        let inner = LsmKvInner::open_indexeddb(init_data, config)?;
        LsmKv::open_with_inner(inner)
    }

    pub fn open_memory() -> DbResult<LsmKv> {
        LsmKv::open_memory_with_config(Config::default())
    }

    pub fn open_memory_with_config(config: Config) -> DbResult<LsmKv> {
        let metrics = LsmMetrics::new();
        let config = Arc::new(config);
        let inner = LsmKvInner::open_with_backend(None, None, metrics, config)?;
        LsmKv::open_with_inner(inner)
    }

    #[inline]
    fn open_with_inner(inner: LsmKvInner) -> DbResult<LsmKv> {
        Ok(LsmKv {
            inner: Arc::new(inner),
        })
    }

    pub fn open_cursor(&self) -> KvCursor {
        let multi_cursor = self.inner.open_multi_cursor(None);
        KvCursor::new(self.inner.clone(), multi_cursor)
    }

    fn open_cursor_with_session(&self, session: Option<&LsmSession>) -> KvCursor {
        let multi_cursor = self.inner.open_multi_cursor(session);
        KvCursor::new(self.inner.clone(), multi_cursor)
    }

    pub(crate) fn open_multi_cursor(&self, session: Option<&LsmSession>) -> MultiCursor {
        self.inner.open_multi_cursor(session)
    }

    pub fn new_session(&self) -> LsmSession {
        let db_ref = Arc::downgrade(&self.inner);
        self.inner.new_session(db_ref)
    }

    pub fn start_transaction(&self) -> DbResult<()> {
        self.inner.indeed_start_transaction(TransactionState::User)
    }

    pub fn put<K, V>(&self, key: K, value: V) -> DbResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut session = self.new_session();
        session.start_transaction(TransactionType::Write)?;
        session.put(key.as_ref(), value.as_ref())?;
        self.inner.commit(&mut session)
    }

    pub fn delete<K>(&self, key: K) -> DbResult<()>
    where
        K: AsRef<[u8]>
    {
        let mut session = self.new_session();
        session.start_transaction(TransactionType::Write)?;
        session.delete(key.as_ref())?;
        self.inner.commit(&mut session)
    }

    pub fn get<'a, K>(&self, key: K) -> DbResult<Option<Arc<[u8]>>>
    where
        K: AsRef<[u8]>,
    {
        self.get_internal(key, None)
    }

    pub fn get_with_session<'a, K>(&self, key: K, session: &LsmSession) -> DbResult<Option<Arc<[u8]>>>
        where
            K: AsRef<[u8]>,
    {
        self.get_internal(key, Some(session))
    }

    #[inline]
    fn get_internal<'a, K>(&self, key: K, session: Option<&LsmSession>) -> DbResult<Option<Arc<[u8]>>>
        where
            K: AsRef<[u8]>,
    {
        let cursor = self.open_cursor_with_session(session);
        cursor.seek(key.as_ref())?;
        let test_key = cursor.key()?;

        match test_key {
            Some(test_key) => {
                if test_key.as_ref().cmp(key.as_ref()) != std::cmp::Ordering::Equal {
                    return Ok(None);
                }
            }
            None => return Ok(None),
        };

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
                let result = String::from_utf8(bytes.to_vec())?;
                Some(result)
            }
        };
        Ok(string)
    }

    pub fn get_string_with_session<'a, K>(&self, key: K, session: &LsmSession) -> DbResult<Option<String>>
        where
            K: AsRef<[u8]>,
    {
        let bytes = self.get_with_session(key, session)?;
        let string = match bytes {
            None => None,
            Some(bytes) => {
                let result = String::from_utf8(bytes.to_vec())?;
                Some(result)
            }
        };
        Ok(string)
    }

    pub fn metrics(&self) -> LsmMetrics {
        self.inner.metrics()
    }

}

pub(crate) struct LsmKvInner {
    backend: Option<Box<dyn LsmBackend>>,
    log: Option<Box<dyn LsmLog>>,
    snapshot: Mutex<Arc<Mutex<LsmSnapshot>>>,
    main_mem_table: Mutex<MemTable>,
    transaction: Mutex<TransactionState>,
    /// Operation count after last sync,
    /// including insert/delete
    op_count: AtomicU64,
    metrics: LsmMetrics,
    pub(crate) config: Arc<Config>,
}

impl LsmKvInner {

    pub(crate) fn read_segment_by_ptr(&self, ptr: LsmTuplePtr) -> DbResult<Arc<[u8]>> {
        let backend = self.backend.as_ref().expect("no file backend");
        backend.read_segment_by_ptr(ptr)
    }

    fn mk_log_path(db_path: &Path) -> PathBuf {
        let mut buf = db_path.to_path_buf();
        let filename = buf.file_name().unwrap().to_str().unwrap();
        let new_filename = String::from(filename) + ".wal";
        buf.set_file_name(new_filename);
        buf
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn open_file(path: &Path, config: Config) -> DbResult<LsmKvInner> {
        let metrics = LsmMetrics::new();
        let config = Arc::new(config);
        let backend = LsmFileBackend::open(path, metrics.clone(), config.clone())?;
        let log_file = LsmKvInner::mk_log_path(path);
        let log = LsmFileLog::open(log_file.as_path(), config.clone())?;
        LsmKvInner::open_with_backend(
            Some(Box::new(backend)),
            Some(Box::new(log)),
            metrics,
            config,
        )
    }

    #[cfg(target_arch = "wasm32")]
    fn open_indexeddb(init_data: JsValue, config: Arc<Config>) -> DbResult<LsmKvInner> {
        let metrics = LsmMetrics::new();
        let backend = IndexeddbBackend::open(
            init_data.clone(),
        )?;

        let session_id = backend.session_id();

        let log = IndexeddbLog::new(session_id, init_data);
        LsmKvInner::open_with_backend(
            Some(Box::new(backend)),
            Some(Box::new(log)),
            metrics,
            config,
        )
    }

    fn open_with_backend(
        backend: Option<Box<dyn LsmBackend>>,
        log: Option<Box<dyn LsmLog>>,
        metrics: LsmMetrics,
        config: Arc<Config>,
    ) -> DbResult<LsmKvInner> {
        let snapshot = match &backend {
            Some(backend) => backend.read_latest_snapshot()?,
            None => LsmSnapshot::new(),
        };
        let mut mem_table = MemTable::new();

        if let Some(log) = &log {
            log.update_mem_table_with_latest_log(
                &snapshot,
                &mut mem_table,
            )?;
        }

        Ok(LsmKvInner {
            backend,
            log,
            snapshot: Mutex::new(Arc::new(Mutex::new(snapshot))),
            main_mem_table: Mutex::new(mem_table),
            transaction: Mutex::new(TransactionState::NoTrans),
            op_count: AtomicU64::new(0),
            metrics,
            config,
        })
    }

    #[inline]
    fn metrics(&self) -> LsmMetrics {
        self.metrics.clone()
    }

    fn open_multi_cursor(&self, session: Option<&LsmSession>) -> MultiCursor {
        let mem_table_cursor = match session {
            Some(session) => {
                session.mem_table.open_cursor()
            }
            None => {
                let mem_table = self.main_mem_table.lock().unwrap();
                mem_table.open_cursor()
            }
        };

        let snapshot_ref = match session {
            Some(session) => session.snapshot.clone(),
            None => self.current_snapshot_ref(),
        };
        let snapshot = snapshot_ref.lock().unwrap();

        let mut cursors: Vec<CursorRepr> = vec![
            mem_table_cursor.into(),
        ];

        if !snapshot.levels.is_empty() {
            // push all cursor on level 0
            let level0 = &snapshot.levels[0];

            for item in level0.content.iter().rev() {
                let cursor = item.segments.open_cursor();
                cursors.push(cursor.into());
            }

            for level in &snapshot.levels[1..] {
                assert_eq!(level.content.len(), 1);
                let cursor = level.content[0].segments.open_cursor();
                cursors.push(cursor.into());
            }
        }

        MultiCursor::new(cursors)
    }

    fn indeed_start_transaction(&self, state: TransactionState) -> DbResult<()> {
        {
            let t_ref = self.transaction.lock()?;
            if *t_ref != TransactionState::NoTrans {
                return Err(DbErr::StartTransactionInAnotherTransaction);
            }
        }

        if let Some(log) = &self.log {
            log.start_transaction()?;
        }

        {
            let mut t_ref = self.transaction.lock()?;
            *t_ref = state;
        }

        Ok(())
    }

    fn new_session(&self, engine: Weak<LsmKvInner>) -> LsmSession {
        let id = (self.op_count.load(Ordering::SeqCst) + 1) as u64;
        let mem_table = {
            let m = self.main_mem_table.lock().unwrap();
            m.clone()
        };
        let snapshot = self.current_snapshot_ref();
        LsmSession::new(
            engine,
            id,
            mem_table,
            snapshot,
            self.log.is_some(),
        )
    }

    #[inline]
    fn is_write_transaction(t: Option<TransactionType>) -> bool {
        if let Some(t) = &t {
            return *t == TransactionType::Write
        }

        false
    }

    #[inline]
    fn current_snapshot_ref(&self) -> Arc<Mutex<LsmSnapshot>>  {
        let ptr = self.snapshot.lock().unwrap();
        ptr.clone()
    }

    #[inline]
    fn set_current_snapshot_ref(&self, v: Arc<Mutex<LsmSnapshot>>) {
        let mut ptr = self.snapshot.lock().unwrap();
        *ptr = v;
    }

    pub(crate) fn commit(&self, session: &mut LsmSession) -> DbResult<()> {
        if !LsmKvInner::is_write_transaction(session.transaction()) {
            session.finished_transaction();
            return Ok(())
        }

        if session.id() != self.op_count.load(Ordering::SeqCst) + 1 {
            return Err(DbErr::SessionOutdated);
        }

        if let Some(log) = &self.log {
            log.start_transaction()?;
            let _commit_result = log.commit(session.log_buffer())?;
            // let mut snapshot = self.snapshot.lock()?;
            // snapshot.log_offset = commit_result.offset;
        }

        let mut mem_table_col = self.main_mem_table.lock()?;
        *mem_table_col = session.mem_table.clone();

        if let Some(backend) = &self.backend {
            let current_snapshot = self.current_snapshot_ref();
            let snapshot_ref: Arc<Mutex<LsmSnapshot>> = if Arc::strong_count(&current_snapshot) == 3 {
                current_snapshot
            } else {
                let cloned = {
                    let origin = current_snapshot.lock()?;
                    origin.clone()
                };
                let snapshot_ref = Arc::new(Mutex::new(cloned));
                self.set_current_snapshot_ref(snapshot_ref.clone());
                session.snapshot = snapshot_ref.clone();

                self.metrics.add_clone_snapshot_count();

                snapshot_ref
            };
            let mut snapshot = snapshot_ref.lock()?;

            let store_bytes = mem_table_col.store_bytes();
            if self.should_sync(store_bytes) {
                backend.sync_latest_segment(
                    &mem_table_col,
                    &mut snapshot,
                )?;
                backend.checkpoint_snapshot(&mut snapshot)?;

                if let Some(log) = &self.log {
                    log.shrink(&mut snapshot)?;
                }

                mem_table_col.clear();

                self.metrics.add_sync_count();
            } else if LsmKvInner::should_minor_compact(&snapshot) {
                self.minor_compact(backend.as_ref(), &mut snapshot)?;
            } else if LsmKvInner::should_major_compact(&snapshot) {
                self.major_compact(backend.as_ref(), &mut snapshot)?;
            }
        }

        self.op_count.store(session.id(), Ordering::SeqCst);
        session.finished_transaction();

        Ok(())
    }

    fn minor_compact(&self, backend: &dyn LsmBackend, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        backend.minor_compact(snapshot)?;
        backend.checkpoint_snapshot(snapshot)?;

        self.metrics.add_minor_compact();

        Ok(())
    }

    fn major_compact(&self, backend: &dyn LsmBackend, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        backend.major_compact(snapshot)?;
        backend.checkpoint_snapshot(snapshot)?;

        self.metrics.add_major_compact();

        Ok(())
    }

    #[inline]
    fn should_sync(&self, store_bytes: usize) -> bool {
        let sync_loc_count = self.config.sync_log_count;
        let op_count = self.op_count.load(Ordering::SeqCst);
        if op_count % sync_loc_count == 0 && op_count != 0 {
            return true;
        }

        let block_size = self.config.lsm_block_size;
        return store_bytes > (block_size as usize);
    }

    #[inline]
    fn should_minor_compact(snapshot: &LsmSnapshot) -> bool {
        if snapshot.levels.is_empty() {
            return false;
        }
        let level0 = &snapshot.levels[0];
        level0.content.len() > 4
    }

    #[inline]
    fn should_major_compact(snapshot: &LsmSnapshot) -> bool {
        snapshot.levels.len() > 4
    }

    // #[allow(dead_code)]
    // pub(crate) fn meta_id(&self) -> u64 {
    //     let snapshot_ref = {
    //         let snapshot_ref = self.snapshot.lock().unwrap();
    //         snapshot_ref.clone()
    //     };
    //     let snapshot = snapshot_ref.lock().unwrap();
    //     snapshot.meta_id
    // }
    //
    fn force_sync_last_segment(&mut self) -> DbResult<()> {
        if let Some(backend) = &self.backend {
            let mem_table = self.main_mem_table.lock().unwrap();
            let snapshot_ref = self.current_snapshot_ref();
            let mut snapshot = snapshot_ref.lock()?;

            if mem_table.len() == 0 {
                return Ok(())
            }

            backend.sync_latest_segment(
                &mem_table,
                &mut snapshot,
            )?;
            backend.checkpoint_snapshot(&mut snapshot)?;
        }

        Ok(())
    }

}

impl Drop for LsmKvInner {

    fn drop(&mut self) {
        let sync_result = self.force_sync_last_segment();
        if sync_result.is_ok() {
            if let Some(log) = &self.log {
                log.enable_safe_clear();
                self.log = None;
            }
        }
    }

}
