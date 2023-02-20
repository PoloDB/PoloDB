use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Mutex;
use bson::Document;
use bson::oid::ObjectId;
use crate::data_ticket::DataTicket;
use crate::{DbErr, DbResult, TransactionType};
use crate::backend::AutoStartResult;
use crate::page::header_page_wrapper::HeaderPageWrapper;
use crate::page::RawPage;
use crate::session::{BaseSession, Session};
use crate::session::session::SessionInner;

struct DynamicSessionInner {
    id: ObjectId,
    version: usize,
    base_session: BaseSession,
    page_map: Option<BTreeMap<u32, RawPage>>,
    page_size: NonZeroU32,
    db_size: u64,
    init_block_count: u64,
}

impl DynamicSessionInner {

    fn new(id: ObjectId, base_session: BaseSession) -> DynamicSessionInner {
        let page_size = base_session.page_size();
        let version = base_session.version();
        let db_size = base_session.db_size();
        let init_block_count = base_session.init_block_count();
        DynamicSessionInner {
            id,
            version,
            base_session,
            page_map: None,
            page_size,
            db_size,
            init_block_count,
        }
    }

    fn start_transaction(&mut self, _ty: TransactionType) -> DbResult<()> {
        if self.page_map.is_some() {
            return Err(DbErr::StartTransactionInAnotherTransaction);
        }
        self.page_map = Some(BTreeMap::new());
        Ok(())
    }

    /// 1. Check version first.
    ///    If the base_session is updated, this commit MUST fail
    /// 2. If the version is valid, flush all the pages to the base
    fn commit(&mut self) -> DbResult<()> {
        let current_version = self.base_session.version();
        if current_version != self.version {
            return Err(DbErr::SessionOutdated);
        }

        if let Some(page_map) = &self.page_map {
            self.base_session.start_transaction(TransactionType::Write)?;
            self.base_session.set_db_size(self.db_size)?;

            for (_page_id, page) in page_map {
                self.base_session.write_page(page)?;
            }

            self.base_session.commit()?;
            self.page_map = None;  // clear the map after commited
            self.version = self.base_session.version();
        }

        Ok(())
    }

    fn rollback(&mut self) -> DbResult<()> {
        if self.page_map.is_none() {
            return Err(DbErr::NoTransactionStarted);
        }
        self.page_map = Some(BTreeMap::new());
        Ok(())
    }
}

impl SessionInner for DynamicSessionInner {
    fn read_page(&mut self, page_id: u32) -> DbResult<RawPage> {
        let page_map = self.page_map.as_ref().ok_or(DbErr::NoTransactionStarted)?;
        match page_map.get(&page_id) {
            Some(page) => Ok(page.clone()),
            None => {
                self.base_session
                    .pipeline_read_page(
                        page_id,
                        Some(&self.id),
                    )
            },
        }
    }

    fn write_page(&mut self, page: &RawPage) -> DbResult<()> {
        let page_map = self.page_map.as_mut().ok_or(DbErr::NoTransactionStarted)?;
        page_map.insert(page.page_id, page.clone());
        Ok(())
    }

    // TODO: refactor with session
    fn actual_alloc_page_id(&mut self) -> DbResult<u32> {
        let first_page = self.get_first_page()?;
        let mut first_page_wrapper = HeaderPageWrapper::from_raw_page(first_page);

        let null_page_bar = first_page_wrapper.get_null_page_bar();
        first_page_wrapper.set_null_page_bar(null_page_bar + 1);

        if (null_page_bar as u64) >= self.db_size {  // truncate file
            let exceed_size = self.init_block_count * (self.page_size().get() as u64);
            self.db_size = exceed_size;
        }

        self.write_page(&first_page_wrapper.0)?;

        crate::polo_log!("alloc new page_id : {}", null_page_bar);

        Ok(null_page_bar)
    }

    fn page_size(&self) -> NonZeroU32 {
        self.page_size
    }
}

pub(crate) struct DynamicSession {
    inner: Mutex<DynamicSessionInner>,
}

impl DynamicSession {

    pub fn new(id: ObjectId, base_session: BaseSession) -> DynamicSession {
        let inner = DynamicSessionInner::new(
            id,
            base_session,
        );
        DynamicSession {
            inner: Mutex::new(inner),
        }
    }

}

impl Session for DynamicSession {
    fn read_page(&self, page_id: u32) -> DbResult<RawPage> {
        let mut inner = self.inner.lock()?;
        inner.read_page(page_id)
    }

    fn write_page(&self, page: &RawPage) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        inner.write_page(page)
    }

    fn page_size(&self) -> NonZeroU32 {
        let inner = self.inner.lock().unwrap();
        inner.page_size()
    }

    fn store_doc(&self, doc: &Document) -> DbResult<DataTicket> {
        let mut inner = self.inner.lock()?;
        inner.store_doc(doc)
    }

    fn alloc_page_id(&self) -> DbResult<u32> {
        let mut inner = self.inner.lock()?;
        inner.alloc_page_id()
    }

    fn free_pages(&self, pages: &[u32]) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        inner.free_pages(pages)
    }

    fn free_data_ticket(&self, data_ticket: &DataTicket) -> DbResult<Vec<u8>> {
        let mut inner = self.inner.lock()?;
        inner.free_data_ticket(data_ticket)
    }

    fn get_doc_from_ticket(&self, data_ticket: &DataTicket) -> DbResult<Option<Document>> {
        let mut inner = self.inner.lock()?;
        inner.get_doc_from_ticket(data_ticket)
    }

    // dynamic session must start transaction manually
    fn auto_start_transaction(&self, _ty: TransactionType) -> DbResult<AutoStartResult> {
        Ok(AutoStartResult {
            auto_start: false,
        })
    }

    fn auto_commit(&self) -> DbResult<()> {
        Ok(())
    }

    fn auto_rollback(&self) -> DbResult<()> {
        Ok(())
    }

    fn start_transaction(&self, ty: TransactionType) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        inner.start_transaction(ty)
    }

    fn commit(&self) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        inner.commit()
    }

    fn rollback(&self) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        inner.rollback()
    }
}
