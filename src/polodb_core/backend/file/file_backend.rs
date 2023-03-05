/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::fs::File;
use std::num::{NonZeroU32, NonZeroU64};
use std::cell::RefCell;
use std::io::{SeekFrom, Seek, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use bson::oid::ObjectId;
use hashbrown::HashMap;
use super::journal_manager::JournalManager;
use super::transaction_state::TransactionState;
use super::pagecache::PageCache;
use crate::backend::Backend;
use crate::{DbResult, DbErr, Config, Metrics};
use crate::page::RawPage;
use crate::page::header_page_wrapper::{HeaderPageWrapper, DATABASE_VERSION};
use crate::transaction::TransactionType;
use crate::error::VersionMismatchError;

pub(crate) struct FileBackend {
    file:            RefCell<File>,
    page_size:       NonZeroU32,
    journal_manager: JournalManager,
    config:          Arc<Config>,
    page_cache:      PageCache,
    state_map:       HashMap<ObjectId, TransactionState>,
    metrics:         Metrics,
}

struct InitDbResult {
    db_file_size: u64,
}

#[cfg(target_os = "windows")]
mod winerror {
    pub const ERROR_SHARING_VIOLATION: i32 = 32;
}

#[cfg(target_os = "windows")]
fn open_file_native(path: &Path) -> DbResult<File> {
    use std::os::windows::prelude::OpenOptionsExt;

    let file_result = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .share_mode(0)
        .open(path);

    match file_result {
        Ok(file) => Ok(file),
        Err(err) => {
            let os_error = err.raw_os_error();
            if let Some(error_code) = os_error {
                if error_code == winerror::ERROR_SHARING_VIOLATION {
                    return Err(DbErr::DatabaseOccupied);
                }
            }
            Err(err.into())
        }
    }
}

#[cfg(not(target_os = "windows"))]
fn open_file_native(path: &Path) -> DbResult<File> {
    use super::file_lock::exclusive_lock_file;
    let file = std::fs::OpenOptions::new()
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

    Ok(file)
}

impl FileBackend {

    fn mk_journal_path(db_path: &Path) -> PathBuf {
        let mut buf = db_path.to_path_buf();
        let filename = buf.file_name().unwrap().to_str().unwrap();
        let new_filename = String::from(filename) + ".journal";
        buf.set_file_name(new_filename);
        buf
    }

    pub(crate) fn open(
        path: &Path,
        page_size: NonZeroU32,
        config: Arc<Config>,
        metrics: Metrics,
    ) -> DbResult<FileBackend> {
        let mut file = open_file_native(path)?;

        let init_result = FileBackend::init_db(
            &mut file,
            page_size,
            config.init_block_count,
            true
        )?;

        let journal_file_path: PathBuf = FileBackend::mk_journal_path(path);
        let journal_manager = JournalManager::open(
            &journal_file_path, page_size, init_result.db_file_size
        )?;

        let page_cache = PageCache::new_default(page_size);

        Ok(FileBackend {
            file: RefCell::new(file),
            page_size,
            journal_manager,
            config,
            page_cache,
            state_map: HashMap::new(),
            metrics,
        })
    }

    fn force_write_first_block(file: &mut File, page_size: NonZeroU32) -> std::io::Result<Arc<RawPage>> {
        let wrapper = HeaderPageWrapper::init(0, page_size);
        wrapper.0.sync_to_file(file, 0)?;
        Ok(Arc::new(wrapper.0))
    }

    fn init_db(file: &mut File, page_size: NonZeroU32, init_block_count: NonZeroU64, check_db_version: bool) -> DbResult<InitDbResult> {
        let meta = file.metadata()?;
        let file_len = meta.len();
        if file_len == 0 {
            let expected_file_size: u64 = (page_size.get() as u64) * init_block_count.get();
            file.set_len(expected_file_size)?;
            FileBackend::force_write_first_block(file, page_size)?;
            Ok(InitDbResult { db_file_size: expected_file_size })
        } else if file_len % page_size.get() as u64 == 0 {
            if check_db_version {
                FileBackend::check_db_version(file)?;
            }
            Ok(InitDbResult { db_file_size: file_len })
        } else {
            Err(DbErr::NotAValidDatabase)
        }
    }

    fn check_db_version(file: &mut File) -> DbResult<()> {
        let mut version = [0u8; 4];
        file.seek(SeekFrom::Start(32))?;
        file.read_exact(&mut version)?;

        if version != DATABASE_VERSION {
            let err = VersionMismatchError {
                expect_version: DATABASE_VERSION,
                actual_version: version,
            };
            return Err(DbErr::VersionMismatch(Box::new(err)))
        }

        Ok(())
    }

    #[inline]
    fn is_journal_full(&self) -> bool {
        (self.journal_manager.len() as u64) >= self.config.journal_full_size
    }

    /// 1. Read the page from the journal
    /// 2. Read the page from the main file
    fn read_page_main(&self, page_id: u32) -> DbResult<Arc<RawPage>> {
        self.metrics.fetch_page();

        if let Some(page) = self.page_cache.get_from_cache(page_id) {
            self.metrics.page_hit_cache();
            return Ok(page);
        }

        let result = {
            if let Some(page) = self.journal_manager.read_page_main(page_id)? {
                return Ok(page);
            }

            self.read_page_from_main_file(page_id)?
        };

        self.page_cache.insert_to_cache(&result);

        Ok(result)
    }

    fn read_page_from_main_file(&self, page_id: u32) -> DbResult<Arc<RawPage>> {
        let offset = (page_id as u64) * (self.page_size.get() as u64);
        let mut result = RawPage::new(page_id, self.page_size);
        let mut main_file = self.file.borrow_mut();

        crate::polo_log!("read page from main file, id: {}", page_id);

        if main_file.seek(SeekFrom::End(0))? >= offset + (self.page_size.get() as u64) {
            result.read_from_file(&mut main_file, offset)?;
        }

        Ok(Arc::new(result))
    }
}

impl Backend for FileBackend {

    fn read_page(&self, page_id: u32, session_id: Option<&ObjectId>) -> DbResult<Arc<RawPage>> {
        match session_id {
            Some(session_id) => {
                let state = self.state_map
                    .get(session_id)
                    .ok_or(DbErr::InvalidSession(Box::new(session_id.clone())))?;
                if let Some(page) = self.journal_manager.read_page(page_id, Some(state))? {
                    return Ok(page);
                }
                self.read_page_from_main_file(page_id)
            }
            None => self.read_page_main(page_id)
        }
    }

    fn write_page(&mut self, page: &RawPage, session_id: Option<&ObjectId>) -> DbResult<()> {
        if session_id.is_some() {
            unreachable!()
        }
        self.journal_manager.append_raw_page(page)?;

        self.page_cache.insert_to_cache(page);

        Ok(())
    }

    /// 1. Write a mark to the journal
    /// 2. If the journal is full, and there is not session is opened,
    ///    merge the journal to the main database.
    fn commit(&mut self) -> DbResult<()> {
        let mut main_db = self.file.borrow_mut();
        self.journal_manager.commit()?;
        if self.is_journal_full() && self.state_map.is_empty() {
            self.journal_manager.checkpoint_journal(&mut main_db)?;
            crate::polo_log!("checkpoint journal finished");
        }
        Ok(())
    }

    fn db_size(&self) -> u64 {
        self.journal_manager.record_db_size()
    }

    fn set_db_size(&mut self, size: u64) -> DbResult<()> {
        self.journal_manager.expand_db_size(size)
    }

    fn transaction_type(&self) -> Option<TransactionType> {
        self.journal_manager.transaction_type()
    }

    fn upgrade_read_transaction_to_write(&mut self) -> DbResult<()> {
        self.journal_manager.upgrade_read_transaction_to_write()
    }

    fn rollback(&mut self) -> DbResult<()> {
        self.journal_manager.rollback()?;
        self.page_cache = PageCache::new_default(self.page_size);
        Ok(())
    }

    fn start_transaction(&mut self, ty: TransactionType) -> DbResult<()> {
        self.journal_manager.start_transaction(ty)
    }

    fn new_session(&mut self, id: &ObjectId) -> DbResult<()> {
        let state = self.journal_manager.new_state(TransactionType::Read);
        self.state_map.insert(id.clone(), state);
        Ok(())
    }

    fn remove_session(&mut self, id: &ObjectId) -> DbResult<()> {
        self.state_map.remove(id);
        Ok(())
    }
}

impl Drop for FileBackend {

    fn drop(&mut self) {
        // release all the session
        self.state_map.clear();

        let mut main_db = self.file.borrow_mut();
        #[cfg(not(target_os = "windows"))]
        let _ = super::file_lock::unlock_file(&main_db);
        let result = self.journal_manager.checkpoint_journal(&mut main_db);
        if result.is_ok() {
            let path = self.journal_manager.path();
            let _ = std::fs::remove_file(path);
        }
    }

}
