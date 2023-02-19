use std::num::NonZeroU32;
use super::RawPage;

static HEADER_DESP: &str          = "PoloDB Format v3.0";
const SECTOR_SIZE_OFFSET: u32     = 40;
const PAGE_SIZE_OFFSET: u32       = 44;
const NULL_PAGE_BAR_OFFSET: u32   = 48;
const META_PAGE_ID: u32           = 52;
const DATA_ALLOCATOR_OFFSET: u32  = 56;
// const META_ID_COUNTER_OFFSET: u32 = 60;
pub const FREE_LIST_OFFSET: u32   = 2048;
const FREE_LIST_PAGE_LINK_OFFSET: u32 = 2048 + 4;
pub const HEADER_FREE_LIST_MAX_SIZE: usize = (2048 - 8) / 4;
pub const DATABASE_VERSION: [u8; 4] = [0, 0, 2, 0];

/**
 * Offset 0 (32 bytes) : "PoloDB Format v3.0";
 * Offset 32 (8 bytes) : Version 0.0.3.0;
 * Offset 40 (4 bytes) : SectorSize;
 * Offset 44 (4 bytes) : PageSize;
 * Offset 48 (4 bytes) : NullPageBarId;
 * Offset 52 (4 bytes) : MetaPageId(usually 1);
 * Offset 56 (4 bytes) : DataAllocatorPageId(0 for none);
 * Offset 60 (4 bytes) : MetaIdCounter;
 *
 * Free list offset: 2048;
 * | 4b   | 4b                  | 4b     | 4b    | ... |
 * | size | free list page link | free 1 | free2 | ... |
 */
pub(crate) struct HeaderPageWrapper(pub RawPage);

impl HeaderPageWrapper {

    pub(crate) fn init(page_id: u32, page_size: NonZeroU32) -> HeaderPageWrapper {
        let raw_page = RawPage::new(page_id, page_size);
        let mut wrapper = HeaderPageWrapper::from_raw_page(raw_page);
        wrapper.set_title(HEADER_DESP);
        wrapper.set_version(&DATABASE_VERSION);
        wrapper.set_sector_size(page_size.get());
        wrapper.set_page_size(page_size.get());
        wrapper.set_meta_page_id(1);
        wrapper.set_null_page_bar(2);
        wrapper
    }

    #[inline]
    pub(crate) fn from_raw_page(page: RawPage) -> HeaderPageWrapper {
        HeaderPageWrapper(page)
    }

    pub(crate) fn set_title(&mut self, title: &str) {
        self.0.seek(0);
        self.0.put_str(title);
    }

    pub(crate) fn get_title(&self) -> String {
        let zero_pos = self.0.data[0..32]
                                .iter()
                                .position(|x| x == &0u8)
                                .expect("can not find a zero");

        let title = String::from_utf8_lossy(&self.0.data[0..zero_pos]);
        title.to_string()
    }

    pub(crate) fn set_version(&mut self, version: &[u8]) {
        self.0.seek(32);
        self.0.put(version);
    }

    #[allow(dead_code)]
    pub(crate) fn get_version(&self) -> [u8; 4] {
        let mut version: [u8; 4] = [0; 4];
        version[..4].clone_from_slice(&self.0.data[32..(4 + 32)]);
        version
    }

    #[inline]
    pub(crate) fn set_sector_size(&mut self, sector_size: u32) {
        self.0.seek(SECTOR_SIZE_OFFSET);
        self.0.put_u32(sector_size);
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn get_sector_size(&self) -> u32 {
        self.0.get_u32(SECTOR_SIZE_OFFSET)
    }

    #[inline]
    pub(crate) fn set_page_size(&mut self, page_size: u32) {
        self.0.seek(PAGE_SIZE_OFFSET);
        self.0.put_u32(page_size);
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn get_page_size(&mut self) -> u32 {
        self.0.get_u32(PAGE_SIZE_OFFSET)
    }

    #[inline]
    pub(crate) fn get_null_page_bar(&self) -> u32 {
        self.0.get_u32(NULL_PAGE_BAR_OFFSET)
    }

    #[inline]
    pub(crate) fn set_null_page_bar(&mut self, data: u32) {
        self.0.seek(NULL_PAGE_BAR_OFFSET);
        self.0.put_u32(data)
    }

    #[inline]
    pub(crate) fn get_meta_page_id(&self) -> u32 {
        self.0.get_u32(META_PAGE_ID)
    }

    #[inline]
    pub(crate) fn set_meta_page_id(&mut self, data: u32) {
        self.0.seek(META_PAGE_ID);
        self.0.put_u32(data)
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn get_data_allocator(&self) -> u32 {
        self.0.get_u32(DATA_ALLOCATOR_OFFSET)
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn set_data_allocator(&mut self, pid: u32) {
        self.0.seek(DATA_ALLOCATOR_OFFSET);
        self.0.put_u32(pid);
    }

    #[inline]
    pub(crate) fn get_free_list_size(&self) -> u32 {
        self.0.get_u32(FREE_LIST_OFFSET)
    }

    #[inline]
    pub(crate) fn set_free_list_size(&mut self, size: u32) {
        self.0.seek(FREE_LIST_OFFSET);
        self.0.put_u32(size)
    }

    #[inline]
    pub(crate) fn get_free_list_content(&self, index: u32) -> u32 {
        let offset = index * 4 + FREE_LIST_OFFSET + 8;
        self.0.get_u32(offset)
    }

    #[inline]
    pub(crate) fn set_free_list_content(&mut self, index: u32, pid: u32) {
        let offset = index * 4 + FREE_LIST_OFFSET + 8;
        self.0.seek(offset);
        self.0.put_u32(pid);
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn set_free_list_page_id(&mut self, pid: u32) {
        self.0.seek(FREE_LIST_PAGE_LINK_OFFSET);
        self.0.put_u32(pid);
    }

    #[inline]
    pub(crate) fn get_free_list_page_id(&self) -> u32 {
        self.0.get_u32(FREE_LIST_PAGE_LINK_OFFSET)
    }

}

#[cfg(test)]
mod tests {
    // use crate::page::HeaderPage;

    use std::num::NonZeroU32;
    use crate::page::RawPage;
    use crate::page::header_page_wrapper::*;

    #[test]
    fn parse_and_gen() {
        let raw_page = RawPage::new(0, NonZeroU32::new(4096).unwrap());

        let mut wrapper = HeaderPageWrapper::from_raw_page(raw_page);

        let title = "test title";
        wrapper.set_title(title);
        assert_eq!(wrapper.get_title(), title);

        let test_sector_size = 111;
        wrapper.set_sector_size(test_sector_size);
        assert_eq!(wrapper.get_sector_size(), test_sector_size);

        let test_page_size = 222;
        wrapper.set_page_size(test_page_size);
        assert_eq!(wrapper.get_page_size(), test_page_size);
    }

}
