use std::fs::{File, Metadata};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::ops::Bound::{Included, Unbounded};
use std::rc::Rc;
use std::path::{Path, PathBuf};
use polodb_bson::Document;
use super::RawPage;
use super::pagecache::PageCache;
use super::header_page_wrapper;
use super::header_page_wrapper::HeaderPageWrapper;
use crate::journal::{JournalManager, TransactionType};
use crate::dump::JournalDump;
use crate::{DbResult, Config};
use crate::error::DbErr;
use crate::page::data_page_wrapper::DataPageWrapper;
use crate::data_ticket::DataTicket;
use crate::page::free_list_data_wrapper::FreeListDataWrapper;

const PRESERVE_WRAPPER_MIN_REMAIN_SIZE: u32 = 16;

#[derive(Eq, PartialEq, Copy, Clone)]
pub(crate) enum TransactionState {
    NoTrans,
    User,
    UserAuto,
    DbAuto,
}

pub(crate) struct PageHandler {
    file:                     File,

    pub page_size:            u32,
    page_cache:               Box<PageCache>,
    journal_manager:          Box<JournalManager>,

    data_page_map:            BTreeMap<u32, Vec<u32>>,

    transaction_state:        TransactionState,

    config:                   Rc<Config>,

}

#[derive(Debug, Copy, Clone)]
pub(crate) struct AutoStartResult {
    pub auto_start: bool,
}

impl PageHandler {

    fn read_first_block(file: &mut File, page_size: u32) -> std::io::Result<RawPage> {
        let mut raw_page = RawPage::new(0, page_size);
        raw_page.read_from_file(file, 0)?;
        Ok(raw_page)
    }

    fn force_write_first_block(file: &mut File, page_size: u32) -> std::io::Result<RawPage> {
        let wrapper = HeaderPageWrapper::init(0, page_size);
        wrapper.0.sync_to_file(file, 0)?;
        Ok(wrapper.0)
    }

    fn init_db(file: &mut File, page_size: u32, init_block_count: u64) -> std::io::Result<(RawPage, u32, u64)> {
        let meta = file.metadata()?;
        let file_len = meta.len();
        if file_len < page_size as u64 {
            let expected_file_size: u64 = (page_size as u64) * init_block_count;
            file.set_len(expected_file_size)?;
            let first_page = PageHandler::force_write_first_block(file, page_size)?;
            Ok((first_page, init_block_count as u32, expected_file_size))
        } else {
            let block_count = file_len / (page_size as u64);
            let first_page = PageHandler::read_first_block(file, page_size)?;
            Ok((first_page, block_count as u32, file_len))
        }
    }

    fn mk_journal_path(db_path: &Path) -> PathBuf {
        let mut buf = db_path.to_path_buf();
        let filename = buf.file_name().unwrap().to_str().unwrap();
        let new_filename = String::from(filename) + ".journal";
        buf.set_file_name(new_filename);
        buf
    }

    #[allow(dead_code)]
    pub fn new(path: &Path, page_size: u32) -> DbResult<PageHandler> {
        let config = Rc::new(Config::default());
        PageHandler::with_config(path, page_size, config)
    }

    pub fn with_config(path: &Path, page_size: u32, config: Rc<Config>) -> DbResult<PageHandler> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;

        let (_, _, db_file_size) = PageHandler::init_db(&mut file, page_size, config.init_block_count)?;

        let journal_file_path: PathBuf = PageHandler::mk_journal_path(path);
        let journal_manager = JournalManager::open(&journal_file_path, page_size, db_file_size)?;

        let page_cache = PageCache::new_default(page_size);

        Ok(PageHandler {
            file,

            page_size,
            page_cache: Box::new(page_cache),
            journal_manager: Box::new(journal_manager),

            data_page_map: BTreeMap::new(),

            transaction_state: TransactionState::NoTrans,

            config,

        })
    }

    pub(crate) fn auto_start_transaction(&mut self, ty: TransactionType) -> DbResult<AutoStartResult> {
        let mut result = AutoStartResult { auto_start: false };
        match self.transaction_state {
            TransactionState::NoTrans => {
                self.start_transaction(ty)?;
                self.transaction_state = TransactionState::DbAuto;
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

    pub(crate) fn auto_rollback(&mut self) -> DbResult<()> {
        if self.transaction_state == TransactionState::DbAuto {
            self.rollback()?;
            self.transaction_state = TransactionState::NoTrans;
        }
        Ok(())
    }

    pub(crate) fn auto_commit(&mut self) -> DbResult<()> {
        if self.transaction_state == TransactionState::DbAuto {
            self.commit()?;
            self.transaction_state = TransactionState::NoTrans;
        }
        Ok(())
    }

    pub(crate) fn distribute_data_page_wrapper(&mut self, data_size: u32) -> DbResult<DataPageWrapper> {
        let data_size = data_size + 2;  // preserve 2 bytes
        let (wrapper, removed_key) = {
            let mut range = self.data_page_map.range_mut((Included(data_size), Unbounded));
            match range.next() {
                Some((key, value)) => {
                    if value.is_empty() {
                        panic!("unexpected: distributed vector is empty");
                    }
                    let last_index = value[value.len() - 1];
                    value.remove(value.len() - 1);

                    let mut removed_key = None;

                    if value.is_empty() {
                        removed_key = Some(*key);
                    }

                    let raw_page = self.pipeline_read_page(last_index)?;
                    let wrapper = DataPageWrapper::from_raw(raw_page);

                    (wrapper, removed_key)
                },
                None => {
                    let wrapper = self.force_distribute_new_data_page_wrapper()?;
                    (wrapper, None)
                },
            }
        };

        if let Some(key) = removed_key {
            self.data_page_map.remove(&key);
        }

        Ok(wrapper)
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

        if wrapper.bar_len() >= (u16::max_value() as u32) / 2 {  // len too large
            return;
        }

        match self.data_page_map.get_mut(&remain_size) {
            Some(vector) => {
                vector.push(wrapper.pid());
            }

            None => {
                let vec = vec![ wrapper.pid() ];
                self.data_page_map.insert(remain_size, vec);
            }
        }
    }

    // 1. write to journal, if success
    //    - 2. checkpoint journal, if full
    // 3. write to page_cache
    pub fn pipeline_write_page(&mut self, page: &RawPage) -> Result<(), DbErr> {
        self.journal_manager.as_mut().append_raw_page(page)?;

        self.page_cache.insert_to_cache(page);
        Ok(())
    }

    // 1. read from page_cache, if none
    // 2. read from journal, if none
    // 3. read from main db
    pub fn pipeline_read_page(&mut self, page_id: u32) -> Result<RawPage, DbErr> {
        if let Some(page) = self.page_cache.get_from_cache(page_id) {
            #[cfg(feature = "log")]
            eprintln!("read page from cache, page_id: {}", page_id);

            return Ok(page);
        }

        if let Some(page) = self.journal_manager.read_page(page_id)? {
            // find in journal, insert to cache
            self.page_cache.insert_to_cache(&page);

            return Ok(page);
        }

        let offset = (page_id as u64) * (self.page_size as u64);
        let mut result = RawPage::new(page_id, self.page_size);

        if self.journal_manager.record_db_size() >= offset + (self.page_size as u64) {
            result.read_from_file(&mut self.file, offset)?;
        }

        self.page_cache.insert_to_cache(&result);

        #[cfg(feature = "log")]
        eprintln!("read page from main file, id: {}", page_id);

        Ok(result)
    }

    pub(crate) fn get_doc_from_ticket(&mut self, data_ticket: &DataTicket) -> DbResult<Option<Rc<Document>>> {
        let page = self.pipeline_read_page(data_ticket.pid)?;
        let wrapper = DataPageWrapper::from_raw(page);
        let bytes = wrapper.get(data_ticket.index as u32);
        if let Some(bytes) = bytes {
            let doc = Document::from_bytes(bytes)?;
            return Ok(Some(Rc::new(doc)));
        }
        Ok(None)
    }

    pub(crate) fn store_doc(&mut self, doc: &Document) -> DbResult<DataTicket> {
        let bytes = doc.to_bytes()?;
        let mut wrapper = self.distribute_data_page_wrapper(bytes.len() as u32)?;
        let index = wrapper.bar_len() as u16;
        let pid = wrapper.pid();
        wrapper.put(&bytes);

        self.pipeline_write_page(wrapper.borrow_page())?;

        self.return_data_page_wrapper(wrapper);

        Ok(DataTicket {
            pid,
            index,
        })
    }

    pub(crate) fn free_data_ticket(&mut self, data_ticket: &DataTicket) -> DbResult<Vec<u8>> {
        #[cfg(feature = "log")]
        eprintln!("free data ticket: {}", data_ticket);

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

    #[inline]
    pub fn free_page(&mut self, pid: u32) -> DbResult<()> {
        self.free_pages(&[pid])
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

    pub fn free_pages(&mut self, pages: &[u32]) -> DbResult<()> {
        #[cfg(feature = "log")]
        for pid in pages {
            eprintln!("free page, id: {}", *pid);
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

    #[inline]
    pub fn is_journal_full(&self) -> bool {
        (self.journal_manager.len() as u64) >= self.config.journal_full_size
    }

    #[inline]
    pub fn checkpoint_journal(&mut self) -> DbResult<()> {
        self.journal_manager.checkpoint_journal(&mut self.file)
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

    pub fn alloc_page_id(&mut self) -> DbResult<u32> {
        let page_id = match self.try_get_free_page_id()? {
            Some(page_id) =>  {

                #[cfg(feature = "log")]
                eprintln!("get new page_id from free list: {}", page_id);

                Ok(page_id)
            }

            None =>  {
                self.actual_alloc_page_id()
            }
        }?;

        Ok(page_id)
    }

    fn actual_alloc_page_id(&mut self) -> DbResult<u32> {
        let first_page = self.get_first_page()?;
        let mut first_page_wrapper = HeaderPageWrapper::from_raw_page(first_page);

        let null_page_bar = first_page_wrapper.get_null_page_bar();
        first_page_wrapper.set_null_page_bar(null_page_bar + 1);

        if (null_page_bar as u64) >= self.journal_manager.record_db_size() {  // truncate file
            let exceed_size = self.config.init_block_count * (self.page_size as u64);
            self.journal_manager.expand_db_size(exceed_size)?;
        }

        self.pipeline_write_page(&first_page_wrapper.0)?;

        #[cfg(feature = "log")]
        eprintln!("alloc new page_id : {}", null_page_bar);

        Ok(null_page_bar)
    }

    #[inline]
    pub fn journal_file_path(&self) -> &Path {
        self.journal_manager.path()
    }

    #[inline]
    pub fn start_transaction(&mut self, ty: TransactionType) -> DbResult<()> {
        self.journal_manager.start_transaction(ty)
    }

    #[inline]
    pub fn transaction_type(&mut self) -> Option<TransactionType> {
        self.journal_manager.transaction_type()
    }

    #[inline]
    fn upgrade_read_transaction_to_write(&mut self) -> DbResult<()> {
        self.journal_manager.upgrade_read_transaction_to_write()
    }

    #[inline]
    pub fn set_transaction_state(&mut self, state: TransactionState) {
        self.transaction_state = state;
    }

    #[inline]
    pub fn transaction_state(&self) -> TransactionState {
        self.transaction_state
    }

    pub fn commit(&mut self) -> DbResult<()> {
        self.journal_manager.commit()?;
        if self.is_journal_full() {
            self.checkpoint_journal()?;
            #[cfg(feature = "log")]
            eprintln!("checkpoint journal finished");
        }
        Ok(())
    }

    // after the rollback
    // all the cache are wrong
    // cleat it
    pub fn rollback(&mut self) -> DbResult<()> {
        self.journal_manager.rollback()?;
        self.page_cache = Box::new(PageCache::new_default(self.page_size));
        Ok(())
    }

    pub fn only_rollback_journal(&mut self) -> DbResult<()> {
        self.journal_manager.rollback()
    }

    #[inline]
    pub fn file_meta(&mut self) -> std::io::Result<Metadata> {
        self.file.metadata()
    }

    pub fn dump_journal(&mut self) -> DbResult<Box<JournalDump>> {
        let journal_dump = self.journal_manager.dump()?;
        Ok(Box::new(journal_dump))
    }

}

#[cfg(test)]
mod test {
    use std::env;
    use crate::page::PageHandler;
    use crate::TransactionType;
    use std::collections::HashSet;

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

        let mut page_handler = PageHandler::new(db_path.as_ref(), 4096).unwrap();
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
