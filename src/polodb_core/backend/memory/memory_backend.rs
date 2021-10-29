use std::num::{NonZeroU32, NonZeroU64};
use std::collections::BTreeMap;
use crate::backend::Backend;
use crate::{DbResult, TransactionType, DbErr};
use crate::page::RawPage;
use crate::page::header_page_wrapper::HeaderPageWrapper;

struct Transaction {
    ty: TransactionType,
    offset_map: BTreeMap<u32, RawPage>,
    db_file_size: u64,
}

impl Transaction {

    pub(super) fn new(ty: TransactionType, db_file_size: u64) -> Transaction {
        Transaction {
            ty,
            offset_map: BTreeMap::new(),
            db_file_size,
        }
    }

}
pub(crate) struct MemoryBackend {
    page_size:   NonZeroU32,
    data:        Vec<u8>,
    transaction: Option<Transaction>,
}

impl MemoryBackend {

    fn force_write_first_block(data: &mut Vec<u8>, page_size: NonZeroU32) {
        let wrapper = HeaderPageWrapper::init(0, page_size);
        let wrapper_size = wrapper.0.data.len();
        data[0..wrapper_size].copy_from_slice(&wrapper.0.data);
    }

    pub(crate) fn new(page_size: NonZeroU32, init_block_count: NonZeroU64) -> MemoryBackend {
        let data_len = init_block_count.get() * (page_size.get() as u64);
        let data_len = data_len as usize;
        let mut data = vec![0; data_len];
        MemoryBackend::force_write_first_block(&mut data, page_size);
        MemoryBackend {
            page_size,
            data,
            transaction: None,
        }
    }

    fn merge_transaction(&mut self) {
        let state = self.transaction.take().unwrap();
        let db_file_size = state.db_file_size;
        self.data.resize(db_file_size as usize, 0);
        for (key, value) in state.offset_map {
            let page_size = self.page_size.get() as usize;
            let start = (key as usize) * page_size;
            let end = start + page_size;
            self.data[start..end].copy_from_slice(&value.data);
        }
    }

    fn recover_file_and_state(&mut self) {
        self.transaction = None;
    }

}

impl Backend for MemoryBackend {
    fn read_page(&self, page_id: u32) -> DbResult<RawPage> {
        if let Some(transaction) = &self.transaction {
            if let Some(page) = transaction.offset_map.get(&page_id) {
                return Ok(page.clone());
            }
        }

        let data_index = (page_id as usize) * (self.page_size.get() as usize);
        let mut result = RawPage::new(page_id, self.page_size);

        let end = data_index + (self.page_size.get() as usize);
        result.data.copy_from_slice(&self.data[data_index..end]);

        Ok(result)
    }

    fn write_page(&mut self, page: &RawPage) -> DbResult<()> {
        match &self.transaction {
            Some(state) if state.ty == TransactionType::Write => (),
            _ => return Err(DbErr::CannotWriteDbWithoutTransaction),
        };

        let state = self.transaction.as_mut().unwrap();

        let page_id = page.page_id;
        state.offset_map.insert(page_id, page.clone());

        let expected_db_size = (page_id as u64 + 1) * (self.page_size.get() as u64);
        if expected_db_size > state.db_file_size {
            state.db_file_size = expected_db_size;
        }

        Ok(())
    }

    fn commit(&mut self) -> DbResult<()> {
        if self.transaction.is_none() {
            return Err(DbErr::CannotWriteDbWithoutTransaction);
        }

        self.merge_transaction();

        Ok(())
    }

    fn db_size(&self) -> u64 {
        self.data.len() as u64
    }

    fn set_db_size(&mut self, _size: u64) -> DbResult<()> {
        todo!()
    }

    fn transaction_type(&self) -> Option<TransactionType> {
        self.transaction.as_ref().map(|state| state.ty)
    }

    fn upgrade_read_transaction_to_write(&mut self) -> DbResult<()> {
        let db_size = self.data.len() as u64;
        let new_state = Transaction::new(TransactionType::Write, db_size);
        self.transaction = Some(new_state);
        Ok(())
    }

    fn rollback(&mut self) -> DbResult<()> {
        if self.transaction.is_none() {
            return Err(DbErr::RollbackNotInTransaction);
        }
        self.recover_file_and_state();
        Ok(())
    }

    fn start_transaction(&mut self, ty: TransactionType) -> DbResult<()> {
        if self.transaction.is_some() {
            return Err(DbErr::Busy);
        }
        let db_size = self.data.len() as u64;
        let new_state = Transaction::new(ty, db_size);
        self.transaction = Some(new_state);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{Config, TransactionType};
    use crate::page::RawPage;
    use crate::backend::memory::MemoryBackend;
    use crate::backend::Backend;
    use std::num::NonZeroU32;

    fn make_raw_page(page_id: u32) -> RawPage {
        let mut page = RawPage::new(
            page_id, NonZeroU32::new(4096).unwrap());

        for i in 0..4096 {
            page.data[i] = unsafe {
                libc::rand() as u8
            }
        }

        page
    }

    static TEST_PAGE_LEN: u32 = 100;

    #[test]
    fn test_commit() {
        let config = Config::default();
        let mut backend = MemoryBackend::new(
            NonZeroU32::new(4096).unwrap(), config.init_block_count
        );

        let mut ten_pages = Vec::with_capacity(TEST_PAGE_LEN as usize);

        for i in 0..TEST_PAGE_LEN {
            ten_pages.push(make_raw_page(i))
        }

        backend.start_transaction(TransactionType::Write).unwrap();
        for item in &ten_pages {
            backend.write_page(item).unwrap();
        }

        backend.commit().unwrap();

        for i in 0..TEST_PAGE_LEN {
            let page = backend.read_page(i).unwrap();

            for (index, ch) in page.data.iter().enumerate() {
                assert_eq!(*ch, ten_pages[i as usize].data[index])
            }
        }

    }

}
