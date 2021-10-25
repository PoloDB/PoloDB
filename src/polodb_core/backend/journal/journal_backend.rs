use std::fs::File;
use std::num::{NonZeroU32, NonZeroU64};
use std::cell::RefCell;
use std::io::{SeekFrom, Seek};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use super::JournalManager;
use crate::file_lock::{exclusive_lock_file, unlock_file};
use crate::backend::Backend;
use crate::{DbResult, DbErr, Config};
use crate::page::RawPage;
use crate::page::header_page_wrapper::HeaderPageWrapper;
use crate::transaction::TransactionType;

pub(crate) struct JournalBackend {
    file:            RefCell<File>,
    page_size:       NonZeroU32,
    journal_manager: RefCell<JournalManager>,
    config:          Rc<Config>,
}

struct InitDbResult {
    db_file_size: u64,
}

impl JournalBackend {

    fn mk_journal_path(db_path: &Path) -> PathBuf {
        let mut buf = db_path.to_path_buf();
        let filename = buf.file_name().unwrap().to_str().unwrap();
        let new_filename = String::from(filename) + ".journal";
        buf.set_file_name(new_filename);
        buf
    }

    pub(crate) fn open(path: &Path, page_size: NonZeroU32, config: Rc<Config>) -> DbResult<JournalBackend> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;

        match exclusive_lock_file(&file) {
            Err(DbErr::Busy) => {
                return Err(DbErr::DatabaseOccupied);
            }
            Err(err) => {
                return Err(err);
            },
            _ => (),
        };

        let init_result = JournalBackend::init_db(&mut file, page_size, config.init_block_count)?;

        let journal_file_path: PathBuf = JournalBackend::mk_journal_path(path);
        let journal_manager = JournalManager::open(
            &journal_file_path, page_size, init_result.db_file_size
        )?;

        Ok(JournalBackend {
            file: RefCell::new(file),
            page_size,
            journal_manager: RefCell::new(journal_manager),
            config,
        })
    }

    fn force_write_first_block(file: &mut File, page_size: NonZeroU32) -> std::io::Result<RawPage> {
        let wrapper = HeaderPageWrapper::init(0, page_size);
        wrapper.0.sync_to_file(file, 0)?;
        Ok(wrapper.0)
    }

    fn init_db(file: &mut File, page_size: NonZeroU32, init_block_count: NonZeroU64) -> DbResult<InitDbResult> {
        let meta = file.metadata()?;
        let file_len = meta.len();
        if file_len == 0 {
            let expected_file_size: u64 = (page_size.get() as u64) * init_block_count.get();
            file.set_len(expected_file_size)?;
            JournalBackend::force_write_first_block(file, page_size)?;
            Ok(InitDbResult { db_file_size: expected_file_size })
        } else if file_len % page_size.get() as u64 == 0 {
            Ok(InitDbResult { db_file_size: file_len })
        } else {
            Err(DbErr::NotAValidDatabase)
        }
    }

    #[inline]
    fn is_journal_full(&self, journal_manager: &JournalManager) -> bool {
        (journal_manager.len() as u64) >= self.config.journal_full_size
    }

}

impl Backend for JournalBackend {

    fn read_page(&self, page_id: u32) -> DbResult<RawPage> {
        let mut journal_manager = self.journal_manager.borrow_mut();
        if let Some(page) = journal_manager.read_page(page_id)? {
            return Ok(page);
        }

        let offset = (page_id as u64) * (self.page_size.get() as u64);
        let mut result = RawPage::new(page_id, self.page_size);
        let mut main_file = self.file.borrow_mut();

        crate::polo_log!("read page from main file, id: {}", page_id);

        if main_file.seek(SeekFrom::End(0))? >= offset + (self.page_size.get() as u64) {
            result.read_from_file(&mut main_file, offset)?;
        }

        Ok(result)
    }

    fn write_page(&mut self, page: &RawPage) -> DbResult<()> {
        let mut journal_manager = self.journal_manager.borrow_mut();
        journal_manager.append_raw_page(page)
    }

    fn commit(&mut self) -> DbResult<()> {
        let mut journal_manager = self.journal_manager.borrow_mut();
        let mut main_db = self.file.borrow_mut();
        journal_manager.commit()?;
        if self.is_journal_full(&journal_manager) {
            journal_manager.checkpoint_journal(&mut main_db)?;
            crate::polo_log!("checkpoint journal finished");
        }
        Ok(())
    }

    fn db_size(&self) -> u64 {
        let journal = self.journal_manager.borrow();
        journal.record_db_size()
    }

    fn set_db_size(&mut self, size: u64) -> DbResult<()> {
        let mut journal = self.journal_manager.borrow_mut();
        journal.expand_db_size(size)
    }

    fn transaction_type(&self) -> Option<TransactionType> {
        let journal = self.journal_manager.borrow();
        journal.transaction_type()
    }

    fn upgrade_read_transaction_to_write(&mut self) -> DbResult<()> {
        let mut journal = self.journal_manager.borrow_mut();
        journal.upgrade_read_transaction_to_write()
    }

    fn rollback(&mut self) -> DbResult<()> {
        let mut journal = self.journal_manager.borrow_mut();
        journal.rollback()
    }

    fn start_transaction(&mut self, ty: TransactionType) -> DbResult<()> {
        let mut journal = self.journal_manager.borrow_mut();
        journal.start_transaction(ty)
    }

}

impl Drop for JournalBackend {

    fn drop(&mut self) {
        let mut journal_manager = self.journal_manager.borrow_mut();
        let mut main_db = self.file.borrow_mut();
        let _ = unlock_file(&main_db);
        let result = journal_manager.checkpoint_journal(&mut main_db);
        if result.is_ok() {
            let path = journal_manager.path();
            let _ = std::fs::remove_file(path);
        }
    }

}
