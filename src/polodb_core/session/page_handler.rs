use std::num::NonZeroU32;
use std::sync::Arc;
use bson::Document;
use std::cmp::min;
use std::cell::Cell;
use crate::backend::{AutoStartResult, Backend};
use crate::{Config, DbErr, DbResult, TransactionType};
use crate::data_ticket::DataTicket;
use crate::dump::JournalDump;
use crate::page::data_page_wrapper::DataPageWrapper;
use crate::page::header_page_wrapper::HeaderPageWrapper;
use crate::page::large_data_page_wrapper::LargeDataPageWrapper;
use crate::page::{FreeListDataWrapper, header_page_wrapper, RawPage};
use crate::transaction::TransactionState;
use super::session::Session;
use super::data_page_allocator::DataPageAllocator;
use super::pagecache::PageCache;

const PRESERVE_WRAPPER_MIN_REMAIN_SIZE: u32 = 16;

pub(crate) struct PageHandler {
    backend:             Box<dyn Backend + Send>,

    pub page_size:       NonZeroU32,
    page_cache:          Box<PageCache>,

    data_page_allocator: DataPageAllocator,

    transaction_state:   TransactionState,

    config:              Arc<Config>,

}

impl PageHandler {

    pub fn new(backend: Box<dyn Backend + Send>, page_size: NonZeroU32, config: Arc<Config>) -> DbResult<PageHandler> {
        let page_cache = PageCache::new_default(page_size);

        Ok(PageHandler {
            backend,

            page_size,
            page_cache: Box::new(page_cache),

            data_page_allocator: DataPageAllocator::new(),

            transaction_state: TransactionState::NoTrans,

            config,

        })
    }

    pub(crate) fn distribute_data_page_wrapper(&mut self, data_size: u32) -> DbResult<DataPageWrapper> {
        let data_size = data_size + 2;  // preserve 2 bytes
        let try_result = self.data_page_allocator.try_allocate_data_page(data_size);
        if let Some((pid, _)) = try_result {
            let raw_page = self.pipeline_read_page(pid)?;
            let wrapper = DataPageWrapper::from_raw(raw_page);
            return Ok(wrapper);
        }
        let wrapper = self.force_distribute_new_data_page_wrapper()?;
        return Ok(wrapper);
    }

    #[inline]
    fn force_distribute_new_data_page_wrapper(&mut self) -> DbResult<DataPageWrapper> {
        let new_pid = self.alloc_page_id()?;
        let new_wrapper = DataPageWrapper::init(new_pid, self.page_size);
        Ok(new_wrapper)
    }

    pub(crate) fn return_data_page_wrapper(&mut self, wrapper: DataPageWrapper) {
        let remain_size = wrapper.remain_size();
        if remain_size < PRESERVE_WRAPPER_MIN_REMAIN_SIZE {
            return;
        }

        if wrapper.bar_len() >= (u16::MAX as u32) / 2 {  // len too large
            return;
        }

        self.data_page_allocator.add_tuple(wrapper.pid(), remain_size);
    }

    #[inline]
    fn pipeline_write_null_page(&mut self, page_id: u32) -> DbResult<()> {
        let page = RawPage::new(page_id, self.page_size);
        self.pipeline_write_page(&page)
    }

    fn get_doc_from_large_page(&mut self, pid: u32) -> DbResult<Option<Document>> {
        let mut bytes: Vec<u8> = Vec::with_capacity(self.page_size.get() as usize);

        let mut next_pid = pid;

        while next_pid != 0 {
            let page = self.pipeline_read_page(next_pid)?;
            let wrapper = LargeDataPageWrapper::from_raw(page);
            wrapper.write_to_buffer(&mut bytes);
            next_pid = wrapper.next_pid();
        }

        let mut my_ref: &[u8] = bytes.as_ref();
        let doc = crate::doc_serializer::deserialize(&mut my_ref)?;
        Ok(Some(doc))
    }

    fn store_large_data(&mut self, bytes: &Vec<u8>) -> DbResult<DataTicket> {
        let mut remain: i64 = bytes.len() as i64;
        let mut pages: Vec<LargeDataPageWrapper> = Vec::new();
        while remain > 0 {
            let new_id = self.alloc_page_id()?;
            let mut large_page_wrapper = LargeDataPageWrapper::init(new_id, self.page_size);
            let max_cap = large_page_wrapper.max_data_cap() as usize;
            let start_index: usize = (bytes.len() as i64 - remain) as usize;
            let end_index: usize = min(start_index + max_cap, bytes.len());
            large_page_wrapper.put(&bytes[start_index..end_index]);

            if pages.len() > 0 {
                let prev = pages.last_mut().unwrap();
                prev.put_next_pid(new_id);
            }
            pages.push(large_page_wrapper);
            remain = (bytes.len() - end_index) as i64;
        }

        let first_pid = pages.first().unwrap().borrow_page().page_id;

        for page in pages {
            self.pipeline_write_page(page.borrow_page())?;
        }

        Ok(DataTicket::large_ticket(first_pid))
    }

    fn free_large_data_page(&mut self, pid: u32) -> DbResult<Vec<u8>> {
        let mut result: Vec<u8> = Vec::with_capacity(self.page_size.get() as usize);
        let mut free_pid: Vec<u32> = Vec::new();

        let mut next_pid = pid;
        while next_pid != 0 {
            free_pid.push(next_pid);

            let page = self.pipeline_read_page(next_pid)?;
            let wrapper = LargeDataPageWrapper::from_raw(page);
            wrapper.write_to_buffer(&mut result);
            next_pid = wrapper.next_pid();
        }

        self.free_pages(&free_pid)?;
        Ok(result)
    }

    // for test
    #[allow(dead_code)]
    fn first_page_free_list_pid_and_size(&mut self) -> DbResult<(u32, u32)> {
        let first_page = self.pipeline_read_page(0)?;
        let first_page_wrapper = HeaderPageWrapper::from_raw_page(first_page);

        let pid = first_page_wrapper.get_free_list_page_id();
        let size = first_page_wrapper.get_free_list_size();
        Ok((pid, size))
    }

    fn internal_free_pages(&mut self, pages: &[u32]) -> DbResult<()> {
        for pid in pages {
            crate::polo_log!("free page, id: {}", *pid);
        }

        let first_page = self.pipeline_read_page(0)?;
        let mut first_page_wrapper = HeaderPageWrapper::from_raw_page(first_page);
        let free_list_pid = first_page_wrapper.get_free_list_page_id();
        if free_list_pid != 0 {
            let cell = Cell::new(free_list_pid);
            self.complex_free_pages(&cell, false, None, pages)?;

            if cell.get() != free_list_pid {  // free list pid changed
                first_page_wrapper.set_free_list_page_id(cell.get());
                self.pipeline_write_page(&first_page_wrapper.0)?;
            }

            return Ok(())
        }

        let current_size = first_page_wrapper.get_free_list_size();
        if (current_size as usize) + pages.len() >= header_page_wrapper::HEADER_FREE_LIST_MAX_SIZE {
            let free_list_pid = self.alloc_page_id()?;
            first_page_wrapper.set_free_list_page_id(free_list_pid);
            self.pipeline_write_page(&first_page_wrapper.0)?;

            let cell = Cell::new(free_list_pid);
            return self.complex_free_pages(&cell, true, None, pages);
        }

        first_page_wrapper.set_free_list_size(current_size + (pages.len() as u32));
        for (counter, pid) in pages.iter().enumerate() {
            first_page_wrapper.set_free_list_content(current_size + (counter as u32), *pid);
        }

        self.pipeline_write_page(&first_page_wrapper.0)?;

        Ok(())
    }

    fn complex_free_pages(&mut self, free_page_id: &Cell<u32>, is_new: bool, next_pid: Option<u32>, pages: &[u32]) -> DbResult<()> {
        let current_free_page_id = free_page_id.get();
        let mut free_list_page_wrapper = if is_new {
            FreeListDataWrapper::init(current_free_page_id, self.page_size)
        } else {
            let raw_page = self.pipeline_read_page(current_free_page_id)?;
            FreeListDataWrapper::from_raw(raw_page)
        };

        if let Some(next_pid) = next_pid {
            free_list_page_wrapper.set_next_pid(next_pid);
        };

        if free_list_page_wrapper.can_store(pages.len()) {
            for pid in pages {
                free_list_page_wrapper.append_page_id(*pid);
            }
            return self.pipeline_write_page(free_list_page_wrapper.borrow_page());
        }

        let new_free_page_pid = self.alloc_page_id()?;

        let remain_size = free_list_page_wrapper.remain_size();
        let front = &pages[0..remain_size as usize];
        let back = &pages[remain_size as usize..];

        let next_cell = Cell::new(new_free_page_pid);
        self.complex_free_pages(&next_cell, true, Some(current_free_page_id), back)?;

        if !front.is_empty() {
            for pid in front {
                free_list_page_wrapper.append_page_id(*pid);
            }

            self.pipeline_write_page(free_list_page_wrapper.borrow_page())?;
        }

        free_page_id.set(next_cell.get());

        Ok(())
    }

    fn try_get_free_page_id(&mut self) -> DbResult<Option<u32>> {
        let first_page = self.get_first_page()?;
        let mut first_page_wrapper = HeaderPageWrapper::from_raw_page(first_page);

        let free_list_page_id = first_page_wrapper.get_free_list_page_id();
        if free_list_page_id != 0 {
            let free_and_next: Cell<i64> = Cell::new(-1);
            let pid = self.get_free_page_id_from_external_page(free_list_page_id, &free_and_next)?;
            if free_and_next.get() >= 0 {
                first_page_wrapper.set_free_list_page_id(free_and_next.get() as u32);
                self.pipeline_write_page(&first_page_wrapper.0)?;
            }
            return Ok(Some(pid));
        }

        let free_list_size = first_page_wrapper.get_free_list_size();
        if free_list_size == 0 {
            return Ok(None);
        }

        let result = first_page_wrapper.get_free_list_content(free_list_size - 1);
        first_page_wrapper.set_free_list_size(free_list_size - 1);

        self.pipeline_write_page(&first_page_wrapper.0)?;

        Ok(Some(result))
    }

    fn get_free_page_id_from_external_page(&mut self, free_list_page_id: u32, free_and_next: &Cell<i64>) -> DbResult<u32> {
        let raw_page = self.pipeline_read_page(free_list_page_id)?;
        let mut free_list_page_wrapper = FreeListDataWrapper::from_raw(raw_page);
        let pid = free_list_page_wrapper.consume_a_free_page();
        if free_list_page_wrapper.size() == 0 {
            let next_pid = free_list_page_wrapper.next_pid();
            self.free_page(pid)?;
            free_and_next.set(next_pid as i64);
        } else {
            self.pipeline_write_page(free_list_page_wrapper.borrow_page())?;
            free_and_next.set(-1);
        }
        Ok(pid)
    }

    #[inline]
    pub fn get_first_page(&mut self) -> Result<RawPage, DbErr> {
        self.pipeline_read_page(0)
    }

    fn actual_alloc_page_id(&mut self) -> DbResult<u32> {
        let first_page = self.get_first_page()?;
        let mut first_page_wrapper = HeaderPageWrapper::from_raw_page(first_page);

        let null_page_bar = first_page_wrapper.get_null_page_bar();
        first_page_wrapper.set_null_page_bar(null_page_bar + 1);

        if (null_page_bar as u64) >= self.backend.db_size() {  // truncate file
            let exceed_size = self.config.init_block_count.get() * (self.page_size.get() as u64);
            self.backend.set_db_size(exceed_size)?;
        }

        self.pipeline_write_page(&first_page_wrapper.0)?;

        crate::polo_log!("alloc new page_id : {}", null_page_bar);

        Ok(null_page_bar)
    }

    #[inline]
    pub fn transaction_type(&mut self) -> Option<TransactionType> {
        self.backend.transaction_type()
    }

    fn upgrade_read_transaction_to_write(&mut self) -> DbResult<()> {
        self.backend.upgrade_read_transaction_to_write()?;
        self.data_page_allocator.start_transaction();
        Ok(())
    }

    #[inline]
    pub fn set_transaction_state(&mut self, state: TransactionState) {
        self.transaction_state = state;
    }

    #[inline]
    pub fn transaction_state(&self) -> &TransactionState {
        &self.transaction_state
    }

    pub fn only_rollback_journal(&mut self) -> DbResult<()> {
        self.backend.rollback()
    }

    pub fn dump_journal(&mut self) -> DbResult<Box<JournalDump>> {
        Err(DbErr::Busy)
    }

    pub fn start_transaction(&mut self, ty: TransactionType) -> DbResult<()> {
        self.backend.start_transaction(ty)?;
        if ty == TransactionType::Write {
            self.data_page_allocator.start_transaction();
        }
        Ok(())
    }

    pub fn commit(&mut self) -> DbResult<()> {
        self.backend.commit()?;
        self.data_page_allocator.commit();
        Ok(())
    }

}

impl Session for PageHandler {

    // 1. read from page_cache, if none
    // 2. read from journal, if none
    // 3. read from main db
    fn pipeline_read_page(&mut self, page_id: u32) -> DbResult<RawPage> {
        if let Some(page) = self.page_cache.get_from_cache(page_id) {
            return Ok(page);
        }

        let result = self.backend.read_page(page_id)?;

        self.page_cache.insert_to_cache(&result);

        Ok(result)
    }

    // 1. write to journal, if success
    //    - 2. checkpoint journal, if full
    // 3. write to page_cache
    fn pipeline_write_page(&mut self, page: &RawPage) -> DbResult<()> {
        self.backend.write_page(page)?;

        self.page_cache.insert_to_cache(page);
        Ok(())
    }

    fn page_size(&self) -> NonZeroU32 {
        self.page_size
    }

    fn store_doc(&mut self, doc: &Document) -> DbResult<DataTicket> {
        let mut bytes = Vec::with_capacity(512);
        crate::doc_serializer::serialize(doc, &mut bytes)?;

        if bytes.len() >= self.page_size.get() as usize / 2 {
            return self.store_large_data(&bytes);
        }

        let mut wrapper = self.distribute_data_page_wrapper(bytes.len() as u32)?;
        let index = wrapper.bar_len() as u16;
        let pid = wrapper.pid();
        if (wrapper.remain_size() as usize) < bytes.len() {
            panic!("page size not enough: {}, bytes: {}", wrapper.remain_size(), bytes.len());
        }
        wrapper.put(&bytes);

        self.pipeline_write_page(wrapper.borrow_page())?;

        self.return_data_page_wrapper(wrapper);

        Ok(DataTicket {
            pid,
            index,
        })
    }

    fn alloc_page_id(&mut self) -> DbResult<u32> {
        let page_id = match self.try_get_free_page_id()? {
            Some(page_id) =>  {
                self.pipeline_write_null_page(page_id)?;

                crate::polo_log!("get new page_id from free list: {}", page_id);

                Ok(page_id)
            }

            None =>  {
                self.actual_alloc_page_id()
            }
        }?;

        Ok(page_id)
    }

    fn free_pages(&mut self, pages: &[u32]) -> DbResult<()> {
        self.internal_free_pages(pages)?;

        for pid in pages {
            self.data_page_allocator.free_page(*pid);
        }

        Ok(())
    }

    fn free_data_ticket(&mut self, data_ticket: &DataTicket) -> DbResult<Vec<u8>> {
        crate::polo_log!("free data ticket: {}", data_ticket);

        if data_ticket.is_large_data() {
            return self.free_large_data_page(data_ticket.pid);
        }

        let page = self.pipeline_read_page(data_ticket.pid)?;
        let mut wrapper = DataPageWrapper::from_raw(page);
        let bytes = wrapper.get(data_ticket.index as u32).unwrap().to_vec();
        wrapper.remove(data_ticket.index as u32);
        if wrapper.is_empty() {
            self.free_page(data_ticket.pid)?;
        }
        let page = wrapper.consume_page();
        self.pipeline_write_page(&page)?;
        Ok(bytes)
    }

    fn get_doc_from_ticket(&mut self, data_ticket: &DataTicket) -> DbResult<Option<Document>> {
        if data_ticket.is_large_data() {
            return self.get_doc_from_large_page(data_ticket.pid);
        }
        let page = self.pipeline_read_page(data_ticket.pid)?;
        let wrapper = DataPageWrapper::from_raw(page);
        let bytes = wrapper.get(data_ticket.index as u32);
        if let Some(bytes) = bytes {
            let mut my_ref: &[u8] = bytes;
            let bytes: &mut &[u8] = &mut my_ref;
            let doc = crate::doc_serializer::deserialize(bytes)?;
            return Ok(Some(doc));
        }
        Ok(None)
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
        self.data_page_allocator.rollback();
        self.page_cache = Box::new(PageCache::new_default(self.page_size));
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::env;
    use std::collections::HashSet;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use crate::backend::file::FileBackend;
    use crate::{Config, TransactionType};
    use crate::session::page_handler::PageHandler;
    use crate::session::Session;

    const TEST_FREE_LIST_SIZE: usize = 10000;
    const DB_NAME: &str = "test-page-handler";

    #[test]
    fn test_free_list() {
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
        let mut page_handler = PageHandler::new(
            backend, page_size, config).unwrap();
        page_handler.start_transaction(TransactionType::Write).unwrap();

        let (free_pid, free_size) = page_handler.first_page_free_list_pid_and_size().unwrap();
        assert_eq!(free_pid, 0);
        assert_eq!(free_size, 0);

        let mut id: Vec<u32> = vec![];
        let mut freed_pid: HashSet<u32> = HashSet::new();

        for _ in 0..TEST_FREE_LIST_SIZE {
            let pid = page_handler.alloc_page_id().unwrap();
            id.push(pid);
        }

        let mut counter = 0;
        for i in id {
            page_handler.free_page(i).expect(&*format!("free page failed: {}", i));
            freed_pid.insert(i);
            let (free_pid, free_size) = page_handler.first_page_free_list_pid_and_size().unwrap();
            if free_pid == 0 {
                assert_eq!(free_size as usize, counter + 1);
            }
            counter += 1;
        }

        let mut counter = 0;
        let mut recover = 0;
        for _ in 0..TEST_FREE_LIST_SIZE {
            let pid = page_handler.alloc_page_id().unwrap();
            if freed_pid.contains(&pid) {
                recover += 1;
                freed_pid.remove(&pid);
            }
            counter += 1;
        }

        page_handler.commit().unwrap();

        let rate = recover as f64 / counter as f64;
        assert!(rate > 0.99, "rate {} too low, pages leak", rate);
    }

}
