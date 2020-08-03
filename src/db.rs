use std::fs::File;
use std::sync::{ Arc, Weak };
use super::page::{ RawPage, ContentPageWrapper, header_page_utils };
use crate::bson::object_id::ObjectIdMaker;
use crate::journal::JournalManager;

static DB_INIT_BLOCK_COUNT: u32 = 8;

pub struct Collection {
    start_page_id:     u32,
}

impl Collection {

    pub fn new(start_page_id: u32) -> Collection {
        Collection { start_page_id }
    }

}

pub struct DbContext {
    pub db_file:      File,

    pub block_size:   u32,
    block_count:      u32,
    pending_block_offset: u32,

    pub first_page:   RawPage,
    pub obj_id_maker: ObjectIdMaker,

    journal_manager:  Box<JournalManager>,
    pub weak_this:    Option<Weak<DbContext>>,
}

pub struct Database {
    ctx: Arc<DbContext>,
}

fn force_write_first_block(file: &mut File, block_size: u32) -> std::io::Result<RawPage> {
    let mut raw_page = RawPage::new(0, block_size);
    header_page_utils::init(&mut raw_page);
    raw_page.sync_to_file(file, 0)?;
    Ok(raw_page)
}

fn init_db(file: &mut File, block_size: u32) -> std::io::Result<(RawPage, u32)> {
    let meta = file.metadata()?;
    let file_len = meta.len();
    if file_len < block_size as u64 {
        file.set_len((block_size as u64) * (DB_INIT_BLOCK_COUNT as u64))?;
        let first_page = force_write_first_block(file, block_size)?;
        Ok((first_page, DB_INIT_BLOCK_COUNT as u32))
    } else {
        let block_count = file_len / (block_size as u64);
        let first_page = read_first_block(file, block_size)?;
        Ok((first_page, block_count as u32))
    }
}

fn read_first_block(file: &mut File, block_size: u32) -> std::io::Result<RawPage> {
    let mut raw_page = RawPage::new(0, block_size);
    raw_page.read_from_file(file, 0)?;
    Ok(raw_page)
}

impl DbContext {

    fn new(path: &str) -> std::io::Result<DbContext> {
        let mut db_file = File::create(path)?;
        let block_size = 4096;
        let (first_page, block_count) = init_db(&mut db_file, block_size)?;
        let obj_id_maker = ObjectIdMaker::new();

        let journal_file_path: String = format!("{}.journal", &path);
        let journal_manager = JournalManager::open(&journal_file_path)?;

        let ctx = DbContext {
            db_file,

            block_size,
            block_count,
            pending_block_offset: 0,

            first_page,
            obj_id_maker,

            journal_manager: Box::new(journal_manager),
            weak_this: None,
        };
        Ok(ctx)
    }

    /**
     * check free list first,
     * if free list is empty, distribte a block from file
     * if file is full, resize the file
     */
    fn alloc_content_page(&mut self) -> std::io::Result<ContentPageWrapper> {
        match self.try_get_free_page_id() {
            Some(page_id) =>  {
                let mut raw_page = RawPage::new(page_id, self.block_size);
                let offset = (self.block_size as u64) * (page_id as u64);
                raw_page.read_from_file(&mut self.db_file, offset)?;

                let weak_ctx = self.weak_this.clone().expect("clone weak ref failed");
                let content_page = ContentPageWrapper::new(weak_ctx, raw_page);
                Ok(content_page)
            }

            None =>  {
                let page_id = 1;  // TODO:
                let raw_page = RawPage::new(page_id, self.block_size);

                let weak_ctx = self.weak_this.clone().expect("clone weak ref failed");
                let content_page = ContentPageWrapper::new(weak_ctx, raw_page);
                Ok(content_page)
            }
        }
    }

    fn try_get_free_page_id(&mut self) -> Option<u32> {
        let free_list_size = header_page_utils::get_free_list_size(&self.first_page);
        if free_list_size == 0 {
            return None;
        }
        let result = header_page_utils::get_free_list_content(&self.first_page, free_list_size - 1);
        header_page_utils::set_free_list_size(&mut self.first_page, free_list_size - 1);
        self.first_page.sync_to_file(&mut self.db_file, 0).expect("sync to file failed");
        Some(result)
    }

}

impl Database {

    pub fn new(path: &str) -> std::io::Result<Database>  {
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

    pub fn create_collection(&mut self, name: &str) -> Collection {
        Collection::new(0)
    }

}
