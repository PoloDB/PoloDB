use std::rc::Rc;
use std::num::NonZeroU32;
use super::memory_journal::MemoryJournal;
use crate::backend::Backend;
use crate::{DbResult, Config, TransactionType};
use crate::page::RawPage;

pub(crate) struct MemoryBackend {
    page_size: NonZeroU32,
    data:      Vec<u8>,
    journal:   MemoryJournal,
}

impl MemoryBackend {

    pub(crate) fn new(page_size: NonZeroU32, config: Rc<Config>) -> MemoryBackend {
        let data_len = config.init_block_count.get() * (page_size.get() as u64);
        let data_len = data_len as usize;
        let data = vec![0; data_len];
        let journal = MemoryJournal::new();
        MemoryBackend {
            page_size,
            data,
            journal,
        }
    }

}

impl Backend for MemoryBackend {
    fn read_page(&self, page_id: u32) -> DbResult<RawPage> {
        if let Some(page) = self.journal.read_page(page_id) {
            return Ok(page);
        }

        let data_index = (page_id as usize) * (self.page_size.get() as usize);
        let mut result = RawPage::new(page_id, self.page_size);

        let end = data_index + (self.page_size.get() as usize);
        result.data.copy_from_slice(&self.data[data_index..end]);

        Ok(result)
    }

    fn write_page(&mut self, page: &RawPage) -> DbResult<()> {
        self.journal.append_raw_page(page);
        Ok(())
    }

    fn commit(&mut self) -> DbResult<()> {
        self.journal.commit()
    }

    fn db_size(&self) -> u64 {
        self.data.len() as u64
    }

    fn set_db_size(&mut self, size: u64) -> DbResult<()> {
        todo!()
    }

    fn transaction_type(&self) -> Option<TransactionType> {
        self.journal.transaction_type()
    }

    fn upgrade_read_transaction_to_write(&mut self) -> DbResult<()> {
        self.journal.upgrade_read_transaction_to_write()
    }

    fn rollback(&mut self) -> DbResult<()> {
        self.journal.rollback()
    }

    fn start_transaction(&mut self, ty: TransactionType) -> DbResult<()> {
        self.journal.start_transaction(ty)
    }
}
