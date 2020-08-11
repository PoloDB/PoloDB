// root_btree schema
// {
//   _id: ObjectId,
//   name: String,
//   root_pid: Int,
//   flags: Int,
// }
//
// flags indicates:
// key_ty: 1byte
// ...
//
use std::fs::File;
use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::collections::LinkedList;
use super::error::DbErr;
use super::pagecache::PageCache;
use super::page::{ RawPage, header_page_utils };
use crate::bson::object_id::ObjectIdMaker;
use crate::journal::JournalManager;
use crate::overflow_data::{ OverflowDataWrapper, OverflowDataTicket };
use crate::bson::{ObjectId, Document, value};
use crate::btree::BTreePageWrapper;

static DB_INIT_BLOCK_COUNT: u32 = 16;

#[derive(Clone)]
pub struct Database {
    ctx: Rc<RefCell<DbContext>>,
}

fn force_write_first_block(file: &mut File, page_size: u32) -> std::io::Result<RawPage> {
    let mut raw_page = RawPage::new(0, page_size);
    header_page_utils::init(&mut raw_page);
    raw_page.sync_to_file(file, 0)?;
    Ok(raw_page)
}

fn init_db(file: &mut File, page_size: u32) -> std::io::Result<(RawPage, u32)> {
    let meta = file.metadata()?;
    let file_len = meta.len();
    if file_len < page_size as u64 {
        file.set_len((page_size as u64) * (DB_INIT_BLOCK_COUNT as u64))?;
        let first_page = force_write_first_block(file, page_size)?;
        Ok((first_page, DB_INIT_BLOCK_COUNT as u32))
    } else {
        let block_count = file_len / (page_size as u64);
        let first_page = read_first_block(file, page_size)?;
        Ok((first_page, block_count as u32))
    }
}

fn read_first_block(file: &mut File, page_size: u32) -> std::io::Result<RawPage> {
    let mut raw_page = RawPage::new(0, page_size);
    raw_page.read_from_file(file, 0)?;
    Ok(raw_page)
}

pub type DbResult<T> = Result<T, DbErr>;

pub(crate) struct DbContext {
    pub db_file:      File,

    pub last_commit_db_size: u64,

    pub page_size:    u32,
    page_count:       u32,
    pending_block_offset: u32,
    overflow_data_pages: LinkedList<u32>,

    page_cache:       PageCache,

    pub obj_id_maker: ObjectIdMaker,

    journal_manager:  Box<JournalManager>,
    pub weak_this:    Option<Weak<RefCell<DbContext>>>,
}

impl DbContext {

    fn new(path: &str) -> DbResult<DbContext> {
        let mut db_file = File::create(path)?;
        let page_size = 4096;
        let (_, page_count) = init_db(&mut db_file, page_size)?;
        let obj_id_maker = ObjectIdMaker::new();

        let journal_file_path: String = format!("{}.journal", &path);
        let journal_manager = JournalManager::open(&journal_file_path, page_size)?;

        let page_cache = PageCache::new_default(page_size);

        let last_commit_db_size = {
            let meta = db_file.metadata()?;
            meta.len()
        };

        let ctx = DbContext {
            db_file,

            last_commit_db_size,

            page_size,
            page_count,
            pending_block_offset: 0,
            overflow_data_pages: LinkedList::new(),

            page_cache,

            // first_page,
            obj_id_maker,

            journal_manager: Box::new(journal_manager),
            weak_this: None,
        };
        Ok(ctx)
    }

    pub fn alloc_page_id(&mut self) -> DbResult<u32> {
        match self.try_get_free_page_id()? {
            Some(page_id) =>  {
                Ok(page_id)
            }

            None =>  {
                self.actual_alloc_page_id()
            }
        }
    }

    fn actual_alloc_page_id(&mut self) -> DbResult<u32> {
        let mut first_page = self.get_first_page()?;

        let null_page_bar = header_page_utils::get_null_page_bar(&first_page);
        header_page_utils::set_null_page_bar(&mut first_page, null_page_bar + 1);

        if (null_page_bar as u64) >= self.last_commit_db_size {  // truncate file
            let expected_size = self.last_commit_db_size + (DB_INIT_BLOCK_COUNT * self.page_size) as u64;

            self.last_commit_db_size = expected_size;
        }

        self.pipeline_write_page(&first_page)?;

        Ok(null_page_bar)
    }

    fn alloc_overflow_ticker(&mut self, size: u32) -> DbResult<OverflowDataTicket> {
        let page_id = self.alloc_page_id()?;

        self.overflow_data_pages.push_back(page_id);

        let raw_page = self.pipeline_read_page(page_id)?;

        let weak_this = self.weak_this.as_ref().expect("not weak this").clone();
        let mut overflow = OverflowDataWrapper::from_raw_page(weak_this, raw_page)?;

        let ticket = overflow.alloc(size)?;

        Ok(OverflowDataTicket {
            items: vec![ ticket ],
        })
    }

    fn try_get_free_page_id(&mut self) -> DbResult<Option<u32>> {
        let mut first_page = self.get_first_page()?;

        let free_list_size = header_page_utils::get_free_list_size(&first_page);
        if free_list_size == 0 {
            return Ok(None);
        }

        let result = header_page_utils::get_free_list_content(&first_page, free_list_size - 1);
        header_page_utils::set_free_list_size(&mut first_page, free_list_size - 1);

        self.pipeline_write_page(&first_page)?;

        Ok(Some(result))
    }

    // 1. write to journal, if success
    //    - 2. checkpoint journal, if full
    // 3. write to page_cache
    pub fn pipeline_write_page(&mut self, page: &RawPage) -> Result<(), DbErr> {
        self.journal_manager.as_mut().append_raw_page(page)?;

        if self.is_journal_full() {
            self.checkpoint_journal()?;
        }

        self.page_cache.insert_to_cache(page);
        Ok(())
    }

    // 1. read from page_cache, if none
    // 2. read from journal, if none
    // 3. read from main db
    pub fn pipeline_read_page(&mut self, page_id: u32) -> Result<RawPage, DbErr> {
        match self.page_cache.get_from_cache(page_id) {
            Some(page) => return Ok(page),
            None => (), // nothing
        }

        match self.journal_manager.read_page(page_id)? {
            Some(page) => {
                // find in journal, insert to cache
                self.page_cache.insert_to_cache(&page);

                return Ok(page);
            }

            None => (),
        }

        let offset = (page_id as u64) * (self.page_size as u64);
        let mut result = RawPage::new(page_id, self.page_size);
        result.read_from_file(&mut self.db_file, offset)?;

        Ok(result)
    }

    pub fn is_journal_full(&self) -> bool {
        self.journal_manager.len() >= 1000
    }

    pub fn checkpoint_journal(&mut self) -> DbResult<()> {
        Err(DbErr::NotImplement)
    }

    #[inline]
    pub fn get_first_page(&mut self) -> Result<RawPage, DbErr> {
        self.pipeline_read_page(0)
    }

    pub fn create_collection(&mut self, name: &str) -> DbResult<ObjectId> {
        let self_rc = self.weak_this.as_ref().unwrap().upgrade().unwrap();

        let oid = self.obj_id_maker.mk_object_id();
        let mut doc = Document::new_without_id();
        doc.insert("_id".into(), value::Value::ObjectId(oid.clone()));

        doc.insert("name".into(), value::Value::String(name.into()));

        let root_pid = self.alloc_page_id()?;
        doc.insert("root_pid".into(), value::Value::Int(root_pid as i64));

        doc.insert("flags".into(), value::Value::Int(0));

        let meta_page_id: u32 = {
            let head_page = self.pipeline_read_page(0)?;
            header_page_utils::get_meta_page_id(&head_page)
        };

        let mut btree_wrapper = BTreePageWrapper::new(self_rc.clone(), meta_page_id);

        let backward = btree_wrapper.insert_item(Rc::new(doc), false)?;

        match backward {
            Some(backward_item) => {
                let new_root_id = self.alloc_page_id()?;

                let raw_page = backward_item.write_to_page(new_root_id, meta_page_id, self.page_size)?;

                // update head page
                {
                    let mut head_page = self.pipeline_read_page(0)?;
                    header_page_utils::set_meta_page_id(&mut head_page, new_root_id);
                    self.pipeline_write_page(&head_page)?;
                }

                self.pipeline_write_page(&raw_page)?;

                Ok(oid)
            }

            None => Ok(oid)
        }
    }

}

impl Drop for DbContext {

    fn drop(&mut self) {
        let _ = self.checkpoint_journal();  // ignored
    }

}

impl Database {

    pub fn open(path: &str) -> DbResult<Database>  {
        let ctx = DbContext::new(path)?;
        let rc_ctx = Rc::new(RefCell::new(ctx));
        let weak_ctx = Rc::downgrade(&rc_ctx);

        {
            // set weak_this
            let cloned = rc_ctx.clone();
            let mut mut_ctx = cloned.borrow_mut();
            mut_ctx.weak_this = Some(weak_ctx);
        }

        Ok(Database {
            ctx: rc_ctx,
        })
    }

    pub fn create_collection(&mut self, name: &str) -> DbResult<ObjectId> {
        let mut ctx = self.ctx.borrow_mut();
        ctx.create_collection(name)
    }

    pub fn get_version(&self) -> String {
        const VERSION: &'static str = env!("CARGO_PKG_VERSION");
        return VERSION.into();
    }

}
