use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Mutex;
use bson::Document;
use crate::data_ticket::DataTicket;
use crate::{DbResult, TransactionType};
use crate::backend::AutoStartResult;
use crate::page::RawPage;
use crate::session::{BaseSession, Session};

struct DynamicSessionInner {
    base_session: BaseSession,
    page_map: BTreeMap<u32, RawPage>,
}

impl DynamicSessionInner {

    fn new(base_session: BaseSession) -> DynamicSessionInner {
        let page_map = BTreeMap::new();
        DynamicSessionInner {
            base_session,
            page_map,
        }
    }

    fn pipeline_read_page(&self, page_id: u32) -> DbResult<RawPage> {
        match self.page_map.get(&page_id) {
            Some(page) => Ok(page.clone()),
            None => self.base_session.pipeline_read_page(page_id),
        }
    }

    fn pipeline_write_page(&mut self, page: &RawPage) -> DbResult<()> {
        self.page_map.insert(page.page_id, page.clone());
        Ok(())
    }
}

pub(crate) struct DynamicSession {
    inner: Mutex<DynamicSessionInner>,
    page_size: NonZeroU32,
}

impl DynamicSession {

    pub fn new(base_session: BaseSession) -> DynamicSession {
        let page_size = base_session.page_size();
        let inner = DynamicSessionInner::new(base_session);
        DynamicSession {
            inner: Mutex::new(inner),
            page_size,
        }
    }

}

impl Session for DynamicSession {
    fn pipeline_read_page(&self, page_id: u32) -> DbResult<RawPage> {
        let inner = self.inner.lock()?;
        inner.pipeline_read_page(page_id)
    }

    fn pipeline_write_page(&self, page: &RawPage) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        inner.pipeline_write_page(page)
    }

    fn page_size(&self) -> NonZeroU32 {
        self.page_size
    }

    fn store_doc(&self, _doc: &Document) -> DbResult<DataTicket> {
        todo!()
    }

    fn alloc_page_id(&self) -> DbResult<u32> {
        todo!()
    }

    fn free_pages(&self, _pages: &[u32]) -> DbResult<()> {
        todo!()
    }

    fn free_data_ticket(&self, _data_ticket: &DataTicket) -> DbResult<Vec<u8>> {
        todo!()
    }

    fn get_doc_from_ticket(&self, _data_ticket: &DataTicket) -> DbResult<Option<Document>> {
        todo!()
    }

    fn auto_start_transaction(&self, _ty: TransactionType) -> DbResult<AutoStartResult> {
        todo!()
    }

    fn auto_commit(&self) -> DbResult<()> {
        todo!()
    }

    fn auto_rollback(&self) -> DbResult<()> {
        todo!()
    }

    fn start_transaction(&self, _ty: TransactionType) -> DbResult<()> {
        todo!()
    }

    fn commit(&self) -> DbResult<()> {
        todo!()
    }

    fn rollback(&self) -> DbResult<()> {
        todo!()
    }
}
