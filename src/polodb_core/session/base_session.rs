use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use bson::Document;
use bson::oid::ObjectId;
use crate::backend::{AutoStartResult, Backend};
use crate::{Config, DbErr, DbResult, Metrics, TransactionType};
use crate::data_ticket::DataTicket;
use crate::dump::JournalDump;
use crate::page::header_page_wrapper::HeaderPageWrapper;
use crate::page::RawPage;
use crate::transaction::TransactionState;
use super::session::{Session, SessionInner};

#[derive(Clone)]
pub(crate) struct BaseSession {
    inner: Arc<Mutex<BaseSessionInner>>,
}

impl BaseSession {

    pub fn new(
        backend: Box<dyn Backend + Send>,
        page_size: NonZeroU32,
        config: Arc<Config>,
        metrics: Metrics,
    ) -> DbResult<BaseSession> {
        let inner = BaseSessionInner::new(
            backend,
            page_size,
            config,
            metrics,
        )?;
        Ok(BaseSession {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub fn transaction_state(&self) -> TransactionState {
        let session = self.inner.as_ref().lock().unwrap();
        session.transaction_state().clone()
    }

    pub fn set_transaction_state(&mut self, state: TransactionState) {
        let mut session = self.inner.as_ref().lock().unwrap();
        session.set_transaction_state(state);
    }

    pub fn dump_journal(&mut self) -> DbResult<Box<JournalDump>> {
        let mut session = self.inner.as_ref().lock()?;
        session.dump_journal()
    }

    pub fn only_rollback_journal(&mut self) -> DbResult<()> {
        let mut session = self.inner.as_ref().lock()?;
        session.only_rollback_journal()
    }

    pub fn new_session(&self, sid: &ObjectId) -> DbResult<()> {
        let mut session = self.inner.as_ref().lock().unwrap();
        session.new_session(sid)
    }

    pub fn remove_session(&self, sid: &ObjectId) -> DbResult<()> {
        let mut session = self.inner.as_ref().lock().unwrap();
        session.remove_session(sid)
    }

    pub fn pipeline_read_page(&self, page_id: u32, session_id: Option<&ObjectId>) -> DbResult<Arc<RawPage>> {
        let mut session = self.inner.as_ref().lock()?;
        session.pipeline_read_page(page_id, session_id)
    }

    #[allow(dead_code)]
    pub fn pipeline_write_page(&mut self, page: &RawPage, session_id: Option<&ObjectId>) -> DbResult<()> {
        let mut session = self.inner.as_ref().lock()?;
        session.pipeline_write_page(page, session_id)
    }

    pub fn version(&self) -> usize {
        let session = self.inner.as_ref().lock().unwrap();
        session.version
    }

    pub fn db_size(&self) -> u64 {
        let session = self.inner.as_ref().lock().unwrap();
        session.db_size()
    }

    pub fn init_block_count(&self) -> u64 {
        let session = self.inner.as_ref().lock().unwrap();
        session.config.init_block_count.get()
    }

    pub fn set_db_size(&self, db_size: u64) -> DbResult<()> {
        let mut session = self.inner.as_ref().lock().unwrap();
        if session.backend.db_size() == db_size {
            return Ok(())
        }
        session.backend.set_db_size(db_size)?;
        Ok(())
    }
}

impl Session for BaseSession {
    fn read_page(&self, page_id: u32) -> DbResult<Arc<RawPage>> {
        let mut session = self.inner.as_ref().lock()?;
        session.read_page(page_id)
    }

    fn write_page(&self, page: &RawPage) -> DbResult<()> {
        let mut session = self.inner.as_ref().lock()?;
        session.write_page(page)
    }

    fn page_size(&self) -> NonZeroU32 {
        let session = self.inner.as_ref().lock().unwrap();
        session.page_size
    }

    fn store_doc(&self, doc: &Document) -> DbResult<DataTicket> {
        let mut session = self.inner.as_ref().lock()?;
        session.store_doc(doc)
    }

    fn store_data_in_storage(&self, data: &[u8]) -> DbResult<DataTicket> {
        let mut session = self.inner.as_ref().lock()?;
        session.store_data_in_storage(data)
    }

    fn alloc_page_id(&self) -> DbResult<u32> {
        let mut session = self.inner.as_ref().lock()?;
        session.alloc_page_id()
    }

    fn free_pages(&self, pages: &[u32]) -> DbResult<()> {
        let mut session = self.inner.as_ref().lock()?;
        session.free_pages(pages)
    }

    fn free_data_ticket(&self, data_ticket: &DataTicket) -> DbResult<Vec<u8>> {
        let mut session = self.inner.as_ref().lock()?;
        session.free_data_ticket(data_ticket)
    }

    fn get_doc_from_ticket(&self, data_ticket: &DataTicket) -> DbResult<Document> {
        let mut session = self.inner.as_ref().lock()?;
        session.get_doc_from_ticket(data_ticket)
    }

    fn get_data_from_storage(&self, data_ticket: &DataTicket) -> DbResult<Vec<u8>> {
        let mut session = self.inner.as_ref().lock()?;
        session.get_data_from_storage(data_ticket)
    }

    fn auto_start_transaction(&self, ty: TransactionType) -> DbResult<AutoStartResult> {
        let mut session = self.inner.as_ref().lock()?;
        session.auto_start_transaction(ty)
    }

    fn auto_commit(&self) -> DbResult<()> {
        let mut session = self.inner.as_ref().lock()?;
        session.auto_commit()
    }

    fn auto_rollback(&self) -> DbResult<()> {
        let mut session = self.inner.as_ref().lock()?;
        session.auto_rollback()
    }

    fn start_transaction(&self, ty: TransactionType) -> DbResult<()> {
        let mut session = self.inner.as_ref().lock()?;
        session.start_transaction(ty)
    }

    fn commit(&self) -> DbResult<()> {
        let mut session = self.inner.as_ref().lock()?;
        session.commit()
    }

    fn rollback(&self) -> DbResult<()> {
        let mut session = self.inner.as_ref().lock()?;
        session.rollback()
    }
}

struct BaseSessionInner {
    version:             usize,
    backend:             Box<dyn Backend + Send>,

    pub page_size:       NonZeroU32,

    transaction_state:   TransactionState,

    config:              Arc<Config>,

    metrics:             Metrics,

}

impl BaseSessionInner {

    fn new(
        backend: Box<dyn Backend + Send>,
        page_size: NonZeroU32,
        config: Arc<Config>,
        metrics: Metrics,
    ) -> DbResult<BaseSessionInner> {
        Ok(BaseSessionInner {
            version: 0,
            backend,
            page_size,

            transaction_state: TransactionState::NoTrans,

            config,

            metrics,
        })
    }

    fn new_session(&mut self, sid: &ObjectId) -> DbResult<()> {
        self.backend.new_session(sid)
    }

    fn remove_session(&mut self, sid: &ObjectId) -> DbResult<()> {
        self.backend.remove_session(sid)
    }

    // for test
    #[allow(dead_code)]
    fn first_page_free_list_pid_and_size(&mut self) -> DbResult<(u32, u32)> {
        let first_page = self.read_page(0)?;
        let first_page_wrapper = HeaderPageWrapper::from_raw_page(first_page.as_ref().clone());

        let pid = first_page_wrapper.get_free_list_page_id();
        let size = first_page_wrapper.get_free_list_size();
        Ok((pid, size))
    }

    #[inline]
    pub fn get_first_page(&mut self) -> Result<Arc<RawPage>, DbErr> {
        self.read_page(0)
    }

    #[inline]
    pub fn transaction_type(&mut self) -> Option<TransactionType> {
        self.backend.transaction_type()
    }

    fn upgrade_read_transaction_to_write(&mut self) -> DbResult<()> {
        self.backend.upgrade_read_transaction_to_write()?;
        Ok(())
    }

    #[inline]
    pub fn db_size(&self) -> u64 {
        self.backend.db_size()
    }

    #[inline]
    fn set_transaction_state(&mut self, state: TransactionState) {
        self.transaction_state = state;
    }

    #[inline]
    pub fn transaction_state(&self) -> &TransactionState {
        &self.transaction_state
    }

    fn only_rollback_journal(&mut self) -> DbResult<()> {
        self.backend.rollback()
    }

    fn dump_journal(&mut self) -> DbResult<Box<JournalDump>> {
        Err(DbErr::Busy)
    }

    fn start_transaction(&mut self, ty: TransactionType) -> DbResult<()> {
        self.backend.start_transaction(ty)?;
        Ok(())
    }

    fn commit(&mut self) -> DbResult<()> {
        self.backend.commit()?;
        self.version += 1;
        Ok(())
    }

    fn auto_start_transaction(&mut self, ty: TransactionType) -> DbResult<AutoStartResult> {
        let mut result = AutoStartResult { auto_start: false };
        match self.transaction_state {
            TransactionState::DbAuto(_) => {
                self.transaction_state.acquire();
            }

            TransactionState::NoTrans => {
                self.start_transaction(ty)?;
                self.transaction_state = TransactionState::new_db_auto();
                result.auto_start = true;
            }

            // current is auto-read, but going to write
            TransactionState::UserAuto => {
                if let (TransactionType::Write, Some(TransactionType::Read)) = (ty, self.transaction_type()) {
                    self.upgrade_read_transaction_to_write()?;
                }
            }

            _ => ()
        }
        Ok(result)
    }

    fn auto_commit(&mut self) -> DbResult<()> {
        if self.transaction_state.release() {
            self.commit()?;
            self.transaction_state = TransactionState::NoTrans;
        }
        Ok(())
    }

    fn auto_rollback(&mut self) -> DbResult<()> {
        if self.transaction_state.release() {
            self.rollback()?;
            self.transaction_state = TransactionState::NoTrans;
        }
        Ok(())
    }

    // after the rollback
    // all the cache are wrong
    // cleat it
    fn rollback(&mut self) -> DbResult<()> {
        self.backend.rollback()?;
        Ok(())
    }


    /// Read page depends on the session
    ///
    /// If the session_id is provided,
    /// read the page from the backend.
    ///
    /// This method will not cache the page for the session.
    ///
    /// Otherwise, the page will read from the main session,
    /// which contains cached pages.
    fn pipeline_read_page(&mut self, page_id: u32, session_id: Option<&ObjectId>) -> DbResult<Arc<RawPage>> {
        match session_id {
            Some(_) => self.backend.read_page(page_id, session_id),
            None => self.pipeline_read_page_main(page_id)
        }
    }

    /// 1. read from page_cache, if none
    /// 2. read from journal, if none
    /// 3. read from main db
    fn pipeline_read_page_main(&mut self, page_id: u32) -> DbResult<Arc<RawPage>> {
        let result = self.backend.read_page(page_id, None)?;
        Ok(result)
    }

    /// Write page depends on the session
    pub fn pipeline_write_page(&mut self, page: &RawPage, session_id: Option<&ObjectId>) -> DbResult<()> {
        match session_id {
            Some(_) => self.backend.write_page(page, session_id),
            None => self.pipeline_write_page_main(page),
        }
    }

    fn pipeline_write_page_main(&mut self, page: &RawPage) -> DbResult<()> {
        self.backend.write_page(page, None)?;
        Ok(())
    }

}

impl SessionInner for BaseSessionInner {
    fn read_page(&mut self, page_id: u32) -> DbResult<Arc<RawPage>> {
        self.pipeline_read_page_main(page_id)
    }

    fn write_page(&mut self, page: &RawPage) -> DbResult<()> {
        self.pipeline_write_page_main(page)
    }

    fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    fn actual_alloc_page_id(&mut self) -> DbResult<u32> {
        let first_page = self.get_first_page()?;
        let mut first_page_wrapper = HeaderPageWrapper::from_raw_page(first_page.as_ref().clone());

        let null_page_bar = first_page_wrapper.get_null_page_bar();
        first_page_wrapper.set_null_page_bar(null_page_bar + 1);

        if (null_page_bar as u64) >= self.backend.db_size() {  // truncate file
            let exceed_size = self.config.init_block_count.get() * (self.page_size.get() as u64);
            self.backend.set_db_size(exceed_size)?;
        }

        self.write_page(&first_page_wrapper.0)?;

        crate::polo_log!("alloc new page_id : {}", null_page_bar);

        Ok(null_page_bar)
    }

    fn page_size(&self) -> NonZeroU32 {
        self.page_size
    }
}

#[cfg(test)]
mod test {
    use std::env;
    use std::collections::HashSet;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use crate::backend::file::FileBackend;
    use crate::{Config, Metrics, TransactionType};
    use crate::session::base_session::BaseSession;
    use crate::session::Session;

    const TEST_FREE_LIST_SIZE: usize = 10000;
    const DB_NAME: &str = "test-page-handler";

    #[test]
    fn test_free_list() {
        let metrics = Metrics::new();
        let mut db_path = env::temp_dir();
        let mut journal_path = env::temp_dir();

        let db_filename = String::from(DB_NAME) + ".db";
        let journal_filename = String::from(DB_NAME) + ".db.journal";

        db_path.push(db_filename);
        journal_path.push(journal_filename);

        let _ = std::fs::remove_file(db_path.as_path());
        let _ = std::fs::remove_file(journal_path);

        let page_size = NonZeroU32::new(4096).unwrap();
        let config = Arc::new(Config::default());
        let backend = Box::new(FileBackend::open(db_path.as_ref(), page_size, config.clone()).unwrap());
        let base_session = BaseSession::new(
            backend, page_size, config, metrics,
        ).unwrap();
        base_session.start_transaction(TransactionType::Write).unwrap();

        let (free_pid, free_size) = {
            let mut inner = base_session.inner.lock().unwrap();
            inner.first_page_free_list_pid_and_size().unwrap()
        };
        assert_eq!(free_pid, 0);
        assert_eq!(free_size, 0);

        let mut id: Vec<u32> = vec![];
        let mut freed_pid: HashSet<u32> = HashSet::new();

        for _ in 0..TEST_FREE_LIST_SIZE {
            let pid = base_session.alloc_page_id().unwrap();
            id.push(pid);
        }

        let mut counter = 0;
        for i in id {
            base_session.free_page(i).expect(&*format!("free page failed: {}", i));
            freed_pid.insert(i);
            let (free_pid, free_size) = {
                let mut inner = base_session.inner.lock().unwrap();
                inner.first_page_free_list_pid_and_size().unwrap()
            };
            if free_pid == 0 {
                assert_eq!(free_size as usize, counter + 1);
            }
            counter += 1;
        }

        let mut counter = 0;
        let mut recover = 0;
        for _ in 0..TEST_FREE_LIST_SIZE {
            let pid = base_session.alloc_page_id().unwrap();
            if freed_pid.contains(&pid) {
                recover += 1;
                freed_pid.remove(&pid);
            }
            counter += 1;
        }

        base_session.commit().unwrap();

        let rate = recover as f64 / counter as f64;
        assert!(rate > 0.99, "rate {} too low, pages leak", rate);
    }

}
