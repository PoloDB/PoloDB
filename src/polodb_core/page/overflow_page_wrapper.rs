use super::page::RawPage;
use crate::page::{PageType, PageHandler};
use crate::DbResult;
use crate::error::DbErr;

pub(crate) struct OverflowPageWrapper(pub RawPage);

static OVERFLOW_PAGE_HEADER_SIZE: u32 = 64;

/**
 * 64 bytes header
 *
 * Offset 0: magic(2 bytes)
 * Offset 4: data_size(4 bytes)
 * Offset 8: next_pid(4 bytes)
 */
impl OverflowPageWrapper {

    fn init(pid: u32, page_size: u32) -> OverflowPageWrapper {
        let raw_page = RawPage::new(pid, page_size);
        let mut wrapper = OverflowPageWrapper(raw_page);

        wrapper.set_magic();

        wrapper
    }

    fn set_magic(&mut self) {
        let page_type = PageType::OverflowData;
        let magic = page_type.to_magic();

        self.0.seek(0);
        self.0.put(&magic);
    }

    #[inline]
    pub(crate) fn get_data_size(&self) -> u32 {
        self.0.get_u32(4)
    }

    pub(crate) fn set_data_size(&mut self, data_size: u32) {
        self.0.seek(4);
        self.0.put_u32(data_size);
    }

    #[inline]
    pub(crate) fn get_next_pid(&self) -> u32 {
        self.0.get_u32(8)
    }

    pub(crate) fn set_next_pid(&mut self, next_pid: u32) {
        self.0.seek(8);
        self.0.put_u32(next_pid);
    }

    #[inline]
    pub fn check_magic(&self) -> bool {
        let page_type = PageType::OverflowData;
        let magic = page_type.to_magic();

        let mut head: [u8; 2] = [0; 2];
        head.copy_from_slice(&self.0.data[0..2]);

        head == magic
    }

    pub fn put_data<'a>(&mut self, data: &'a [u8]) -> Option<&'a [u8]> {
        let full_page_size = self.0.len();
        let remain_data_cap = full_page_size - OVERFLOW_PAGE_HEADER_SIZE;
        let overflow_size = (data.len() as isize) - (remain_data_cap as isize);
        if overflow_size > 0 {  // continue overflow
            let current_slice = &data[0..(remain_data_cap as usize)];
            let remain_slice = &data[(remain_data_cap as usize)..];

            self.set_data_size(remain_data_cap);
            self.0.seek(OVERFLOW_PAGE_HEADER_SIZE);
            self.0.put(current_slice);

            return Some(remain_slice);
        }

        self.set_data_size(data.len() as u32);
        self.0.seek(OVERFLOW_PAGE_HEADER_SIZE);
        self.0.put(data);

        None
    }

    pub fn recursively_get_overflow_data(page_handler: &mut PageHandler, buffer: &mut Vec<u8>, overflow_pid: u32) -> DbResult<()> {
        let overflow_page = page_handler.pipeline_read_page(overflow_pid)?;
        let overflow_page_wrapper = OverflowPageWrapper(overflow_page);
        if !overflow_page_wrapper.check_magic() {
            return Err(DbErr::PageMagicMismatch(overflow_pid));
        }

        let current_page_data_size = overflow_page_wrapper.get_data_size() as usize;
        let current_page_next_pid = overflow_page_wrapper.get_next_pid();

        buffer.extend_from_slice(&overflow_page_wrapper.0.data[0..current_page_data_size]);

        if current_page_next_pid == 0 {
            return Ok(())
        }

        OverflowPageWrapper::recursively_get_overflow_data(page_handler, buffer, current_page_next_pid)
    }

    pub fn recursively_free_page(page_handler: &mut PageHandler, overflow_pid: u32) -> DbResult<()> {
        let mut page_ids: Vec<u32> = vec![];

        OverflowPageWrapper::recursively_get_free_pids(page_handler, &mut page_ids, overflow_pid)?;

        page_handler.free_pages(&page_ids)
    }

    fn recursively_get_free_pids(page_handler: &mut PageHandler, pids: &mut Vec<u32>, current_pid: u32) -> DbResult<()> {
        let overflow_page = page_handler.pipeline_read_page(current_pid)?;
        let overflow_page_wrapper = OverflowPageWrapper(overflow_page);

        let next_pid = overflow_page_wrapper.get_next_pid();

        pids.push(current_pid);

        if next_pid == 0 {
            return Ok(());
        }

        OverflowPageWrapper::recursively_get_free_pids(page_handler, pids, next_pid)
    }

    pub fn handle_overflow<'a>(page_handler: &mut PageHandler, data: &'a [u8], item_content_size: usize) -> DbResult<(&'a [u8], u32)> {
        if data.len() <= item_content_size {
            return Ok((data, 0));
        }

        let ret_data = &data[0..item_content_size];
        let tail_data = &data[item_content_size..];

        let allow_pid = page_handler.alloc_page_id()?;
        let mut wrapper = OverflowPageWrapper::init(allow_pid, page_handler.page_size);

        match wrapper.put_data(tail_data) {
            Some(_next_slices) => {
                return Err(DbErr::NotImplement);
            },

            None => ()
        };

        return Ok((ret_data, 0));
    }

}

#[cfg(test)]
mod tests {
    use crate::page::{RawPage, OverflowPageWrapper};
    use crate::page::overflow_page_wrapper::OVERFLOW_PAGE_HEADER_SIZE;

    #[test]
    fn test_overflow_page() {
        let page = RawPage::new(1, 4096);
        let mut wrapper = OverflowPageWrapper(page);

        let buffer = [0 ; 100];

        assert_eq!(wrapper.put_data(&buffer), None);

        let buffer: [u8; 4096] = [0; 4096];
        let return_buffer = wrapper.put_data(&buffer).unwrap();

        assert_eq!(return_buffer.len(), (OVERFLOW_PAGE_HEADER_SIZE as usize));
    }

}
