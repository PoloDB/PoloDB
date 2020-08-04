use std::fs::File;
use std::sync::{ Arc, Weak };
use super::error::DbErr;
use super::pagecache::PageCache;
use super::page::{ RawPage, ContentPageWrapper, header_page_utils };
use crate::bson::object_id::ObjectIdMaker;
use crate::journal::JournalManager;

static DB_INIT_BLOCK_COUNT: u32 = 8;

// pub struct Collection {
//     start_page_id:     u32,
// }
//
// pub struct CreateCollectionOptions {
//     capped: bool,
//     max:    u32,
// }
//
// impl Collection {
//
//     pub fn new(start_page_id: u32) -> Collection {
//         Collection { start_page_id }
//     }
//
// }

pub struct Database {
    ctx: Arc<DbContext>,
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

pub struct DbContext {
    pub db_file:      File,

    pub page_size:    u32,
    page_count:       u32,
    pending_block_offset: u32,

    page_cache:       PageCache,

    pub obj_id_maker: ObjectIdMaker,

    journal_manager:  Box<JournalManager>,
    pub weak_this:    Option<Weak<DbContext>>,
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

        let ctx = DbContext {
            db_file,

            page_size,
            page_count,
            pending_block_offset: 0,

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

        // TODO: check bar

        self.pipeline_write_page(&first_page)?;

        Ok(null_page_bar)
    }

    /**
     * check free list first,
     * if free list is empty, distribte a block from file
     * if file is full, resize the file
     */
    // fn alloc_content_page(&mut self) -> DbResult<ContentPageWrapper> {
    //     match self.try_get_free_page_id()? {
    //         Some(page_id) =>  {
    //             let raw_page = self.pipeline_read_page(page_id)?;
    //
    //             let weak_ctx = self.weak_this.clone().expect("clone weak ref failed");
    //             let content_page = ContentPageWrapper::new(weak_ctx, raw_page);
    //             Ok(content_page)
    //         }
    //
    //         None =>  {
    //             let page_id = 1;  // TODO:
    //             let raw_page = RawPage::new(page_id, self.page_size);
    //
    //             let weak_ctx = self.weak_this.clone().expect("clone weak ref failed");
    //             let content_page = ContentPageWrapper::new(weak_ctx, raw_page);
    //             Ok(content_page)
    //         }
    //     }
    // }

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

}

impl Drop for DbContext {

    fn drop(&mut self) {
        let _ = self.checkpoint_journal();  // ignored
    }

}

impl Database {

    pub fn new(path: &str) -> DbResult<Database>  {
        let ctx = DbContext::new(path)?;
        let mut rc_ctx: Arc<DbContext> = Arc::new(ctx);
        let weak_ctx = Arc::downgrade(&rc_ctx);

        // set weak_this
        let mut_ctx = Arc::get_mut(&mut rc_ctx).expect("get mut ctx failed");
        mut_ctx.weak_this = Some(weak_ctx);

        Ok(Database {
            ctx: rc_ctx,
        })
    }

    // pub fn create_collection(&mut self, name: &str) -> Collection {
    //     Collection::new(0)
    // }

}
