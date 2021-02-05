use std::num::NonZeroU32;
use crate::page::{RawPage, PageType};

const SIZE_OFFSET: u32 = 4;
const NEXT_PAGE_OFFSET: u32 = 8;
const DATA_FRAGMENT_OFFSET: u32 = 16;

/**
 * Offset 0 (2 bytes): magic number
 *
 * Offset 4 (4 bytes): size in current page
 * Offset 8 (4 bytes): next free list data wrapper
 * Offset 16: data begin
 */
pub(crate) struct FreeListDataWrapper {
    page: RawPage,
}

impl FreeListDataWrapper {

    pub(crate) fn init(page_id: u32, page_size: NonZeroU32) -> FreeListDataWrapper {
        let mut raw_page = RawPage::new(page_id, page_size);
        let page_type = PageType::FreeList;
        raw_page.put(&page_type.to_magic());

        FreeListDataWrapper {
            page: raw_page,
        }
    }

    pub(crate) fn from_raw(raw_page: RawPage) -> FreeListDataWrapper {
        FreeListDataWrapper {
            page: raw_page
        }
    }

    pub(crate) fn remain_size(&self) -> u32 {
        let size_cap = (self.page.len() - DATA_FRAGMENT_OFFSET) / 4;
        size_cap - self.size()
    }

    pub(crate) fn append_page_id(&mut self, pid: u32) {
        debug_assert_ne!(self.remain_size(), 0);
        let current_size = self.size();
        let data_offset = DATA_FRAGMENT_OFFSET + current_size * 4;
        self.page.seek(data_offset);
        self.page.put_u32(pid);
        self.set_size(current_size + 1);
    }

    #[inline]
    pub(crate) fn size(&self) -> u32 {
        self.page.get_u32(SIZE_OFFSET)
    }

    pub(crate) fn set_size(&mut self, value: u32) {
        self.page.seek(SIZE_OFFSET);
        self.page.put_u32(value);
    }

    #[inline]
    pub(crate) fn next_pid(&self) -> u32 {
        self.page.get_u32(NEXT_PAGE_OFFSET)
    }

    pub(crate) fn set_next_pid(&mut self, value: u32) {
        self.page.seek(NEXT_PAGE_OFFSET);
        self.page.put_u32(value);
    }

    pub(crate) fn consume_a_free_page(&mut self) -> u32 {
        let size = self.size();
        assert_ne!(size, 0, "no free data is zero");

        let pid = self.get_pid_by_index(size - 1);

        self.set_size(size - 1);

        pid
    }

    fn get_pid_by_index(&self, index: u32) -> u32 {
        let data_offset: u32 = DATA_FRAGMENT_OFFSET + index * 4;
        self.page.get_u32(data_offset)
    }

    #[inline]
    pub(crate) fn can_store(&self, len: usize) -> bool {
        (self.remain_size() as usize) >= len
    }

    #[inline]
    pub(crate) fn borrow_page(&self) -> &RawPage {
        &self.page
    }

}
