use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Mutex;
use bson::Document;
use bson::oid::ObjectId;
use crate::data_ticket::DataTicket;
use crate::{DbErr, DbResult, TransactionType};
use crate::backend::AutoStartResult;
use crate::page::data_page_wrapper::DataPageWrapper;
use crate::page::RawPage;
use crate::session::{BaseSession, Session};
use crate::session::session::SessionInner;

struct DynamicSessionInner {
    id: ObjectId,
    base_session: BaseSession,
    page_map: Option<BTreeMap<u32, RawPage>>,
    page_size: NonZeroU32,
}

impl DynamicSessionInner {

    fn new(id: ObjectId, base_session: BaseSession) -> DynamicSessionInner {
        let page_size = base_session.page_size();
        DynamicSessionInner {
            id,
            base_session,
            page_map: None,
            page_size,
        }
    }

    fn start_transaction(&mut self, _ty: TransactionType) -> DbResult<()> {
        if self.page_map.is_some() {
            return Err(DbErr::StartTransactionInAnotherTransaction);
        }
        self.page_map = Some(BTreeMap::new());
        Ok(())
    }

    // 1. check version first, if the base_session is updated, this commit MUST fail
    // 2. if the version is valid, flush all the pages to the base
    fn commit(&mut self) -> DbResult<()> {
        todo!()
    }

    fn rollback(&mut self) -> DbResult<()> {
        if self.page_map.is_none() {
            return Err(DbErr::NoTransactionStarted);
        }
        self.page_map = None;
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

    fn distribute_data_page_wrapper(&mut self, _data_size: u32) -> DbResult<DataPageWrapper> {
        todo!()
    }

    fn return_data_page_wrapper(&mut self, _wrapper: DataPageWrapper) {
        todo!()
    }

    fn actual_alloc_page_id(&mut self) -> DbResult<u32> {
        todo!()
    }

    fn free_pages(&mut self, _pages: &[u32]) -> DbResult<()> {
        todo!()
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
