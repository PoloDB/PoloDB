use std::rc::Rc;
use std::num::NonZeroU32;
use std::collections::BTreeMap;
use crate::backend::Backend;
use crate::{DbResult, Config, TransactionType, DbErr};
use crate::page::RawPage;

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

    pub(crate) fn new(page_size: NonZeroU32, config: Rc<Config>) -> MemoryBackend {
        let data_len = config.init_block_count.get() * (page_size.get() as u64);
        let data_len = data_len as usize;
        let data = vec![0; data_len];
        MemoryBackend {
            page_size,
            data,
            transaction: None,
        }
    }

    fn merge_transaction(&mut self) {
        unimplemented!()
    }

    fn recover_file_and_state(&mut self) {
        unimplemented!()
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

        let expected_db_size = (page_id as u64) * (self.page_size.get() as u64);
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
