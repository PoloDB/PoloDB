use std::num::NonZeroU32;
use bson::Document;
use crate::data_ticket::DataTicket;
use crate::{DbResult, TransactionType};
use crate::backend::AutoStartResult;
use crate::page::RawPage;

pub(crate) trait Session {
    fn pipeline_read_page(&mut self, page_id: u32) -> DbResult<RawPage>;
    fn pipeline_write_page(&mut self, page: &RawPage) -> DbResult<()>;
    fn page_size(&self) -> NonZeroU32;
    fn store_doc(&mut self, doc: &Document) -> DbResult<DataTicket>;
    fn alloc_page_id(&mut self) -> DbResult<u32>;
    fn free_pages(&mut self, pages: &[u32]) -> DbResult<()>;
    fn free_page(&mut self, pid: u32) -> DbResult<()> {
        self.free_pages(&[pid])
    }
    fn free_data_ticket(&mut self, data_ticket: &DataTicket) -> DbResult<Vec<u8>>;
    fn get_doc_from_ticket(&mut self, data_ticket: &DataTicket) -> DbResult<Option<Document>>;
    fn auto_start_transaction(&mut self, ty: TransactionType) -> DbResult<AutoStartResult>;
    fn auto_commit(&mut self) -> DbResult<()>;
    fn auto_rollback(&mut self) -> DbResult<()>;
    fn rollback(&mut self) -> DbResult<()>;
}
