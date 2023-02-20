use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use bson::oid::ObjectId;
use hashbrown::HashMap;
use crate::backend::Backend;
use crate::{DbResult, TransactionType, DbErr};
use crate::backend::memory::db_snapshot::{DbSnapshot, DbSnapshotDraft};
use crate::page::RawPage;
use crate::page::header_page_wrapper::HeaderPageWrapper;

struct Transaction {
    ty: TransactionType,
    draft: DbSnapshotDraft,
}

impl Transaction {

    pub(super) fn new(ty: TransactionType, snapshot: DbSnapshot) -> Transaction {
        let draft = DbSnapshotDraft::new(snapshot);
        Transaction {
            ty,
            draft,
        }
    }

}

pub(crate) struct MemoryBackend {
    page_size:   NonZeroU32,
    snapshot:    DbSnapshot,
    transaction: Option<Transaction>,
    state_map:   HashMap<ObjectId, Transaction>,
}

impl MemoryBackend {

    fn force_write_first_block(snapshot: DbSnapshot, page_size: NonZeroU32) -> DbSnapshot {
        let wrapper = HeaderPageWrapper::init(0, page_size);

        let mut snapshot_draft = DbSnapshotDraft::new(snapshot);
        snapshot_draft.write_page(&wrapper.0);

        snapshot_draft.commit()
    }

    pub(crate) fn new(page_size: NonZeroU32, init_block_count: NonZeroU64) -> MemoryBackend {
        let data_len = init_block_count.get() * (page_size.get() as u64);
        let snapshot = MemoryBackend::force_write_first_block(
            DbSnapshot::new(page_size, data_len),
            page_size
        );
        MemoryBackend {
            page_size,
            snapshot,
            transaction: None,
            state_map: HashMap::new(),
        }
    }

    fn merge_transaction(&mut self) {
        let state = self.transaction.take().unwrap();
        self.snapshot = state.draft.commit();
    }

    fn recover_file_and_state(&mut self) {
        self.transaction = None;
    }

    fn read_page_main(&self, page_id: u32) -> DbResult<Arc<RawPage>> {
        if let Some(transaction) = &self.transaction {
            if let Some(page) = transaction.draft.read_page(page_id) {
                return Ok(page);
            }
        }

        let test_page = self.snapshot.read_page(page_id);

        if test_page.is_none() {
            let page_size = self.page_size.get() as u64;
            let db_file_size = self.db_size();
            if (page_id as u64) * page_size < db_file_size {
                let null_page = RawPage::new(page_id, self.page_size);
                return Ok(Arc::new(null_page));
            }
        }

        let page = test_page.expect(format!("page not exist: {}", page_id).as_str());
        Ok(page)
    }
}

impl Backend for MemoryBackend {
    fn read_page(&self, page_id: u32, session_id: Option<&ObjectId>) -> DbResult<Arc<RawPage>> {
        match session_id {
            Some(session_id) => {
                // read the page from the state
                let state = self.state_map
                    .get(session_id)
                    .ok_or(DbErr::InvalidSession(Box::new(session_id.clone())))?;
                let test_page = state.draft.read_page(page_id);

                if test_page.is_none() {
                    let page_size = self.page_size.get() as u64;
                    let db_file_size = state.draft.db_file_size();
                    if (page_id as u64) * page_size < db_file_size {
                        let null_page = RawPage::new(page_id, self.page_size);
                        return Ok(Arc::new(null_page));
                    }
                }

                Ok(test_page.unwrap())
            }
            None => self.read_page_main(page_id),
        }
    }

    fn write_page(&mut self, page: &RawPage, session_id: Option<&ObjectId>) -> DbResult<()> {
        if session_id.is_some() {
            unimplemented!()
        }

        match &self.transaction {
            Some(state) if state.ty == TransactionType::Write => (),
            _ => return Err(DbErr::CannotWriteDbWithoutTransaction),
        };

        let page_id = page.page_id;
        let state = self.transaction.as_mut().unwrap();
        state.draft.write_page(page);

        let expected_db_size = (page_id as u64 + 1) * (self.page_size.get() as u64);
        if expected_db_size > state.draft.db_file_size() {
            state.draft.set_db_file_size(expected_db_size);
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
        if let Some(transaction) = &self.transaction {
            transaction.draft.db_file_size()
        } else {
            self.snapshot.db_file_size()
        }
    }

    fn set_db_size(&mut self, size: u64) -> DbResult<()> {
        if let Some(transaction) = &mut self.transaction {
            transaction.draft.set_db_file_size(size);
        }
        Ok(())
    }

    fn transaction_type(&self) -> Option<TransactionType> {
        self.transaction.as_ref().map(|state| state.ty)
    }

    fn upgrade_read_transaction_to_write(&mut self) -> DbResult<()> {
        let new_state = Transaction::new(
            TransactionType::Write,
            self.snapshot.clone(),
        );
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
        let new_state = Transaction::new(ty, self.snapshot.clone());
        self.transaction = Some(new_state);

        Ok(())
    }

    fn new_session(&mut self, id: &ObjectId) -> DbResult<()> {
        let transaction = Transaction::new(
            TransactionType::Read,
            self.snapshot.clone(),
        );
        self.state_map.insert(id.clone(), transaction);
        Ok(())
    }

    fn remove_session(&mut self, id: &ObjectId) -> DbResult<()> {
        self.state_map.remove(id);
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
            backend.write_page(item, None).unwrap();
        }

        backend.commit().unwrap();

        for i in 0..TEST_PAGE_LEN {
            let page = backend.read_page_main(i).unwrap();

            for (index, ch) in page.data.iter().enumerate() {
                assert_eq!(*ch, ten_pages[i as usize].data[index])
            }
        }

    }

}
