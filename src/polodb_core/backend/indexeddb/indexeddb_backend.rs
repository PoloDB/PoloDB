use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use bson::oid::ObjectId;
use crate::backend::Backend;
use crate::backend::memory::MemoryBackend;
use crate::{DbResult, TransactionType};
use crate::page::RawPage;

pub(crate) struct IndexedDbBackend {
    mem: MemoryBackend,
}

impl IndexedDbBackend {

    pub fn open(page_size: NonZeroU32, init_block_count: NonZeroU64) -> IndexedDbBackend {
        IndexedDbBackend {
            mem: MemoryBackend::new(page_size, init_block_count),
        }
    }

}

impl Backend for IndexedDbBackend {
    fn read_page(&self, page_id: u32, session_id: Option<&ObjectId>) -> DbResult<Arc<RawPage>> {
        self.mem.read_page(page_id, session_id)
    }

    fn write_page(&mut self, page: &RawPage, session_id: Option<&ObjectId>) -> DbResult<()> {
        self.mem.write_page(page, session_id)
    }

    fn commit(&mut self) -> DbResult<()> {
        self.mem.commit()
    }

    fn db_size(&self) -> u64 {
        self.mem.db_size()
    }

    fn set_db_size(&mut self, size: u64) -> DbResult<()> {
        self.mem.set_db_size(size)
    }

    fn transaction_type(&self) -> Option<TransactionType> {
        self.mem.transaction_type()
    }

    fn upgrade_read_transaction_to_write(&mut self) -> DbResult<()> {
        self.mem.upgrade_read_transaction_to_write()
    }

    fn rollback(&mut self) -> DbResult<()> {
        self.mem.rollback()
    }

    fn start_transaction(&mut self, ty: TransactionType) -> DbResult<()> {
        self.mem.start_transaction(ty)
    }

    fn new_session(&mut self, id: &ObjectId) -> DbResult<()> {
        self.mem.new_session(id)
    }

    fn remove_session(&mut self, id: &ObjectId) -> DbResult<()> {
        self.mem.remove_session(id)
    }
}
