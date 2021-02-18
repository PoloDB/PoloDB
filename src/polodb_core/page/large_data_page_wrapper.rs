use std::num::NonZeroU32;
use crate::page::{RawPage, PageType};

/**
 * Offset 0 (2 bytes): magic number
 * Offset 2 (2 bytes): data len
 * Offset 4 (4 bytes): next page id
 * Offset 8: data begin
 */
pub(crate) struct LargeDataPageWrapper {
    page: RawPage
}

impl LargeDataPageWrapper {

    pub(crate) fn init(page_id: u32, page_size: NonZeroU32) -> LargeDataPageWrapper {
        let mut raw_page = RawPage::new(page_id, page_size);
        let page_type = PageType::LargeData;
        raw_page.put(&page_type.to_magic());

        LargeDataPageWrapper {
            page: raw_page,
        }
    }

    pub(crate) fn from_raw(raw_page: RawPage) -> LargeDataPageWrapper {
        LargeDataPageWrapper {
            page: raw_page
        }
    }

    pub(crate) fn put_next_pid(&mut self, next_pid: u32) {
        self.page.seek(4);
        self.page.put_u32(next_pid);
    }

    pub(crate) fn max_data_cap(&self) -> u32 {
        self.page.len() - 8
    }

    pub(crate) fn put(&mut self, data: &[u8]) {
        debug_assert!(data.len() <= self.max_data_cap() as usize);
        self.page.seek(2);
        self.page.put_u16(data.len() as u16);
        self.page.seek(8);
        self.page.put(data);
    }

    pub(crate) fn next_pid(&self) -> u32 {
        self.page.get_u32(4)
    }

    pub(crate) fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        let len = self.page.get_u16(2) as usize;
        let start_index: usize = 8;
        let end_index = start_index + len;
        buffer.extend_from_slice(&self.page.data[start_index..end_index]);
    }

    #[inline]
    pub(crate) fn borrow_page(&self) -> &RawPage {
        &self.page
    }

}
