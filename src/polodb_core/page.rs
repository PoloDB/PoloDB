use std::fs::File;
use std::io::{Seek, SeekFrom, Write, Read};
use crate::pagecache::PageCache;
use crate::journal::JournalManager;
use crate::DbResult;
use crate::error::{DbErr, parse_error_reason};

static DB_INIT_BLOCK_COUNT: u32 = 16;

#[repr(u8)]
#[allow(dead_code)]
pub(crate) enum PageType {
    Undefined = 0,

    BTreeNode,

    OverflowData,

}

impl PageType {

    pub fn to_magic(self) -> [u8; 2] {
        [0xFF, self as u8]
    }

    pub fn from_magic(magic: [u8; 2]) -> DbResult<PageType> {
        if magic[0] != 0xFF {
            return Err(DbErr::ParseError(parse_error_reason::UNEXPECTED_PAGE_HEADER.into()));
        }

        match magic[1] {
            0 => Ok(PageType::Undefined),

            1 => Ok(PageType::BTreeNode),

            2 => Ok(PageType::OverflowData),

            _ => Err(DbErr::ParseError(parse_error_reason::UNEXPECTED_PAGE_TYPE.into()))
        }
    }

}

pub(crate) struct PageHandler {
    file:                     File,

    pub last_commit_db_size:  u64,

    pub page_size:            u32,
    page_count:               u32,
    page_cache:               PageCache,
    journal_manager:          Box<JournalManager>,
}

impl PageHandler {

    fn read_first_block(file: &mut File, page_size: u32) -> std::io::Result<RawPage> {
        let mut raw_page = RawPage::new(0, page_size);
        raw_page.read_from_file(file, 0)?;
        Ok(raw_page)
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
            let first_page = PageHandler::force_write_first_block(file, page_size)?;
            Ok((first_page, DB_INIT_BLOCK_COUNT as u32))
        } else {
            let block_count = file_len / (page_size as u64);
            let first_page = PageHandler::read_first_block(file, page_size)?;
            Ok((first_page, block_count as u32))
        }
    }

    pub fn new(path: &str, page_size: u32) -> DbResult<PageHandler> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;

        let (_, page_count) = PageHandler::init_db(&mut file, page_size)?;

        let journal_file_path: String = format!("{}.journal", &path);
        let journal_manager = JournalManager::open(&journal_file_path, page_size)?;

        let page_cache = PageCache::new_default(page_size);

        let last_commit_db_size = {
            let meta = file.metadata()?;
            meta.len()
        };

        Ok(PageHandler {
            file,

            last_commit_db_size,

            page_size,
            page_count,
            page_cache,
            journal_manager: Box::new(journal_manager),
        })
    }

    // 1. write to journal, if success
    //    - 2. checkpoint journal, if full
    // 3. write to page_cache
    pub fn pipeline_write_page(&mut self, page: &RawPage) -> Result<(), DbErr> {
        self.journal_manager.as_mut().append_raw_page(page)?;

        if self.is_journal_full() {
            self.checkpoint_journal()?;
            #[cfg(feature = "log")]
                eprintln!("checkpoint journal finished");
        }

        self.page_cache.insert_to_cache(page);
        Ok(())
    }

    // 1. read from page_cache, if none
    // 2. read from journal, if none
    // 3. read from main db
    pub fn pipeline_read_page(&mut self, page_id: u32) -> Result<RawPage, DbErr> {
        match self.page_cache.get_from_cache(page_id) {
            Some(page) => {
                #[cfg(feature = "log")]
                eprintln!("read page from cache, page_id: {}", page_id);

                return Ok(page);
            },
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
        result.read_from_file(&mut self.file, offset)?;

        self.page_cache.insert_to_cache(&result);

        #[cfg(feature = "log")]
            eprintln!("read page from main file, id: {}", page_id);

        Ok(result)
    }

    pub fn free_page(&mut self, pid: u32) -> DbResult<()> {
        #[cfg(feature = "log")]
            eprintln!("free page, id: {}", pid);

        let mut first_page = self.pipeline_read_page(0)?;
        let free_list_pid = header_page_utils::get_free_list_page_id(&first_page);
        if free_list_pid != 0 {
            return Err(DbErr::NotImplement);
        }

        let current_size = header_page_utils::get_free_list_size(&first_page);
        if (current_size as usize) >= header_page_utils::HEADER_FREE_LIST_MAX_SIZE {
            return Err(DbErr::NotImplement)
        }

        header_page_utils::set_free_list_content(&mut first_page, current_size, pid);
        header_page_utils::set_free_list_size(&mut first_page, current_size + 1);

        self.pipeline_write_page(&first_page)
    }

    pub fn is_journal_full(&self) -> bool {
        self.journal_manager.len() >= 1000
    }

    pub fn checkpoint_journal(&mut self) -> DbResult<()> {
        self.journal_manager.checkpoint_journal(&mut self.file)
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

    #[inline]
    pub fn get_first_page(&mut self) -> Result<RawPage, DbErr> {
        self.pipeline_read_page(0)
    }

    pub fn alloc_page_id(&mut self) -> DbResult<u32> {
        match self.try_get_free_page_id()? {
            Some(page_id) =>  {

                #[cfg(feature = "log")]
                    eprintln!("get new page_id from free list: {}", page_id);

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

        #[cfg(feature = "log")]
            eprintln!("alloc new page_id : {}", null_page_bar);

        Ok(null_page_bar)
    }

    #[inline]
    pub fn start_transaction(&mut self) -> DbResult<()> {
        self.journal_manager.start_transaction()
    }

    #[inline]
    pub fn commit(&mut self) -> DbResult<()> {
        self.journal_manager.commit()
    }

    #[inline]
    pub fn rollback(&mut self) -> DbResult<()> {
        self.journal_manager.rollback()
    }

}

#[derive(Debug)]
pub(crate) struct RawPage {
    pub page_id:    u32,
    pub data:       Vec<u8>,
    pos:            u32,
}

impl RawPage {

    pub fn new(page_id: u32, size: u32) -> RawPage {
        let mut v: Vec<u8> = Vec::new();
        v.resize(size as usize, 0);
        RawPage {
            page_id,
            data: v,
            pos: 0,
        }
    }

    pub unsafe fn copy_from_ptr(&mut self, ptr: *const u8) {
        let target_ptr = self.data.as_mut_ptr();
        target_ptr.copy_from_nonoverlapping(ptr, self.data.len());
    }

    pub unsafe fn copy_to_ptr(&self, ptr: *mut u8) {
        let target_ptr = self.data.as_ptr();
        target_ptr.copy_to_nonoverlapping(ptr, self.data.len());
    }

    pub fn put(&mut self, data: &[u8]) {
        if data.len() + self.pos as usize > self.data.len() {
            panic!("space is not enough for page");
        }

        unsafe {
            self.data.as_mut_ptr().offset(self.pos as isize)
                .copy_from_nonoverlapping(data.as_ptr(), data.len());
        }

        self.pos += data.len() as u32;
    }

    pub fn put_str(&mut self, str: &str) {
        if str.len() + self.pos as usize > self.data.len() {
            panic!("space is not enough for page");
        }

        unsafe {
            self.data.as_mut_ptr().offset(self.pos as isize).copy_from_nonoverlapping(str.as_ptr(), str.len());
        }

        self.pos += str.len() as u32;
    }

    #[allow(dead_code)]
    pub fn get_u8(&self, pos: u32) -> u8 {
        self.data[pos as usize]
    }

    #[inline]
    #[allow(dead_code)]
    pub fn put_u8(&mut self, data: u8) {
        self.data[self.pos as usize] = data
    }

    #[inline]
    pub fn get_u16(&self, pos: u32) -> u16 {
        let mut buffer: [u8; 2] = [0; 2];
        buffer.copy_from_slice(&self.data[(pos as usize)..((pos as usize) + 2)]);
        u16::from_be_bytes(buffer)
    }

    #[inline]
    pub fn put_u16(&mut self, data: u16) {
        let data_be = data.to_be_bytes();
        self.put(&data_be)
    }

    #[inline]
    pub fn get_u32(&self, pos: u32) -> u32 {
        let mut buffer: [u8; 4] = [0; 4];
        buffer.copy_from_slice(&self.data[(pos as usize)..((pos as usize) + 4)]);
        u32::from_be_bytes(buffer)
    }

    #[inline]
    pub fn put_u32(&mut self, data: u32) {
        let data_be = data.to_be_bytes();
        self.put(&data_be)
    }

    #[inline]
    pub fn put_u64(&mut self, data: u64) {
        let data_be = data.to_be_bytes();
        self.put(&data_be)
    }

    #[inline]
    #[allow(dead_code)]
    pub fn get_u64(&self, pos: u32) -> u64 {
        let mut buffer: [u8; 8] = [0; 8];
        buffer.copy_from_slice(&self.data[(pos as usize)..((pos as usize) + 8)]);
        u64::from_be_bytes(buffer)
    }

    pub fn sync_to_file(&self, file: &mut File, offset: u64) -> std::io::Result<()> {
        file.seek(SeekFrom::Start(offset))?;
        file.write(self.data.as_slice())?;
        Ok(())
    }

    pub fn read_from_file(&mut self, file: &mut File, offset: u64) -> std::io::Result<()> {
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(self.data.as_mut_slice())?;
        Ok(())
    }

    #[inline]
    pub fn seek(&mut self, pos: u32) {
        self.pos = pos;
    }

    #[inline]
    #[allow(dead_code)]
    pub fn len(&self) -> u32 {
        self.data.len() as u32
    }

}

struct FreeList {
    free_list_page_id:   u32,
    data:                Vec<u32>,
}

impl FreeList {

    fn new() -> FreeList {
        FreeList {
            free_list_page_id: 0,
            data: Vec::new(),
        }
    }

    fn from_raw(raw_page: &RawPage) -> FreeList {
        let size = raw_page.get_u32(header_page_utils::FREE_LIST_OFFSET);
        let free_list_page_id = raw_page.get_u32(header_page_utils::FREE_LIST_OFFSET + 4);

        let mut data: Vec<u32> = Vec::new();
        data.resize(size as usize, 0);

        for i in 0..size {
            let offset = header_page_utils::FREE_LIST_OFFSET + 8 + (i * 4);
            data.insert(i as usize, raw_page.get_u32(offset));
        }

        FreeList {
            free_list_page_id,
            data,
        }
    }
    
}

/**
 * Offset 0 (32 bytes) : "PipeappleDB Format v0.1";
 * Offset 32 (8 bytes) : Version 0.0.0.0;
 * Offset 40 (4 bytes) : SectorSize;
 * Offset 44 (4 bytes) : PageSize;
 * Offset 48 (4 bytes) : NullPageBarId;
 * Offset 52 (4 bytes) : MetaPageId(usually 1);
 *
 * Free list offset: 2048;
 * | 4b   | 4b                  | 4b     | 4b    | ... |
 * | size | free list page link | free 1 | free2 | ... |
 */
pub mod header_page_utils {
    use crate::page::RawPage;

    static HEADER_DESP: &str         = "PipeappleDB Format v0.1";
    static SECTOR_SIZE_OFFSET: u32   = 40;
    static PAGE_SIZE_OFFSET: u32     = 44;
    static NULL_PAGE_BAR_OFFSET: u32 = 48;
    static META_PAGE_ID: u32         = 52;
    pub static FREE_LIST_OFFSET: u32 = 2048;
    static FREE_LIST_PAGE_LINK_OFFSET: u32 = 2048 + 4;
    pub static HEADER_FREE_LIST_MAX_SIZE: usize = (2048 - 8) / 4;

    pub(crate) fn init(page: &mut RawPage) {
        set_title(page, HEADER_DESP);
        set_version(page, &[0, 0, 0, 1]);
        set_sector_size(page, 4096);
        set_page_size(page, 4096);
        set_meta_page_id(page, 1);
        set_null_page_bar(page, 2);
    }

    pub(crate) fn set_title(page: &mut RawPage, title: &str) {
        page.seek(0);
        let _ = page.put_str(title);
    }

    pub(crate) fn get_title(page: &RawPage) -> String {
        let mut zero_pos: i32 = -1;
        for i in 0..32 {
            if page.data[i] == 0 {
                zero_pos = i as i32;
                break;
            }
        }

        if zero_pos < 0 {
            panic!("can not find a zero")
        }

        let title = String::from_utf8_lossy(&page.data[0..(zero_pos as usize)]);
        title.to_string()
    }

    pub(crate) fn set_version(page: &mut RawPage, version: &[u8]) {
        page.seek(32);
        let _ = page.put(version);
    }

    pub(crate) fn get_version(page: &RawPage) -> [u8; 4] {
        let mut version: [u8; 4] = [0; 4];
        for i in 0..4 {
            version[i] = page.data[32 + i];
        }
        version
    }

    #[inline]
    pub(crate) fn set_sector_size(page: &mut RawPage, sector_size: u32) {
        page.seek(SECTOR_SIZE_OFFSET);
        let _ = page.put_u32(sector_size);
    }

    #[inline]
    pub(crate) fn get_sector_size(page: &RawPage) -> u32 {
        page.get_u32(SECTOR_SIZE_OFFSET)
    }

    #[inline]
    pub(crate) fn set_page_size(page: &mut RawPage, page_size: u32) {
        page.seek(PAGE_SIZE_OFFSET);
        let _ = page.put_u32(page_size);
    }

    #[inline]
    pub(crate) fn get_page_size(page: &RawPage) -> u32 {
        page.get_u32(PAGE_SIZE_OFFSET)
    }

    #[inline]
    pub(crate) fn get_null_page_bar(page: &RawPage) -> u32 {
        page.get_u32(NULL_PAGE_BAR_OFFSET)
    }

    #[inline]
    pub(crate) fn set_null_page_bar(page: &mut RawPage, data: u32) {
        page.seek(NULL_PAGE_BAR_OFFSET);
        page.put_u32(data)
    }

    #[inline]
    pub(crate) fn get_meta_page_id(page: &RawPage) -> u32 {
        page.get_u32(META_PAGE_ID)
    }

    #[inline]
    pub(crate) fn set_meta_page_id(page: &mut RawPage, data: u32) {
        page.seek(META_PAGE_ID);
        page.put_u32(data)
    }

    #[inline]
    pub(crate) fn get_free_list_size(page: &RawPage) -> u32 {
        page.get_u32(FREE_LIST_OFFSET)
    }

    #[inline]
    pub(crate) fn set_free_list_size(page: &mut RawPage, size: u32) {
        page.seek(FREE_LIST_OFFSET);
        page.put_u32(size)
    }

    #[inline]
    pub(crate) fn get_free_list_content(page: &RawPage, index: u32) -> u32 {
        let offset = index * 4 + FREE_LIST_OFFSET + 8;
        page.get_u32(offset)
    }

    #[inline]
    pub(crate) fn set_free_list_content(page: &mut RawPage, index: u32, pid: u32) {
        let offset = index * 4 + FREE_LIST_OFFSET + 8;
        page.seek(offset);
        page.put_u32(pid);
    }

    #[inline]
    pub(crate) fn set_free_list_page_id(page: &mut RawPage, pid: u32) {
        page.seek(FREE_LIST_PAGE_LINK_OFFSET);
        page.put_u32(pid);
    }

    #[inline]
    pub(crate) fn get_free_list_page_id(page: &RawPage) -> u32 {
        page.get_u32(FREE_LIST_PAGE_LINK_OFFSET)
    }

    #[cfg(test)]
    mod tests {
        // use crate::page::HeaderPage;

        use crate::page::RawPage;
        use crate::page::header_page_utils::*;

        #[test]
        fn parse_and_gen() {
            let mut raw_page = RawPage::new(0, 4096);

            let title = "test title";
            set_title(&mut raw_page, title);
            assert_eq!(get_title(&raw_page), title);

            let test_sector_size = 111;
            set_sector_size(&mut raw_page, test_sector_size);
            assert_eq!(get_sector_size(&raw_page), test_sector_size);

            let test_page_size = 222;
            set_page_size(&mut raw_page, test_page_size);
            assert_eq!(get_page_size(&raw_page), test_page_size);
        }

    }

}
