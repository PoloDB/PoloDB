use std::cmp::{min, Ordering};
use std::num::NonZeroU32;
use crate::page::{PageType, RawPage};

/// | 4 bytes | 4 bytes      |
/// | pid     | remain size  |
const ITEM_SIZE: u32 = 8;
const HEADER_SIZE: u32 = 8;

pub(crate) struct DataAllocatorWrapper {
    free_pages: Vec<(u32, u32)>,
    page_id: u32,
    page_size: NonZeroU32,
}

impl DataAllocatorWrapper {

    /// Offset 0 (2 bytes): magic number
    /// Offset 2 (2 bytes): data len
    /// Offset 4 (4 bytes): next page id(preserved)
    /// Offset 8: data begin
    pub fn from_raw_page(page: &RawPage) -> DataAllocatorWrapper {
        let data_len = page.get_u16(2) as u32;

        let mut free_pages = Vec::with_capacity(data_len as usize);

        for index in 0..data_len {
            let offset = 8 + ITEM_SIZE * index;
            let pid = page.get_u32(offset as u32);
            let remain_size = page.get_u32(offset as u32 + 4);
            free_pages.push((pid, remain_size));
        }

        let page_id = page.page_id;
        let page_size = page.len();
        DataAllocatorWrapper {
            free_pages,
            page_id,
            page_size: NonZeroU32::new(page_size).unwrap(),
        }
    }

    pub fn new(page_id: u32, page_size: NonZeroU32) -> DataAllocatorWrapper {
        DataAllocatorWrapper {
            free_pages: Vec::new(),
            page_id,
            page_size,
        }
    }

    pub fn try_allocate_data_page(&mut self, need_size: u32) -> Option<(u32, u32)> {
        let mut index: i64 = -1;
        let mut result = None;
        for (i, (pid, remain_size)) in self.free_pages.iter().enumerate() {
            if *remain_size >= need_size {
                index = i as i64;
                result = Some((*pid, *remain_size));
                break;
            }
        }
        if index >= 0 {
            self.free_pages.remove(index as usize);
        }

        result
    }

    pub fn generate_page(&mut self) -> RawPage {
        self.sort();

        let mut raw_page = RawPage::new(self.page_id, self.page_size);

        let page_type = PageType::DataAllocator;
        raw_page.put(&page_type.to_magic());

        let max_size = self.max_size();
        let final_size = min(max_size, self.free_pages.len());

        raw_page.seek(2);
        raw_page.put_u16(final_size as u16);

        raw_page.seek(HEADER_SIZE as u32);

        for i in 0..final_size {
            let (pid, remain_size) = self.free_pages[i];
            raw_page.put_u32(pid);
            raw_page.put_u32(remain_size);
        }

        raw_page
    }

    pub fn push(&mut self, pid: u32, remain_size: u32) {
        self.free_pages.push((pid, remain_size));
    }

    fn sort(&mut self) {
        self.free_pages.sort_by(|a, b| {
            let (a_pid, a_remain_size) = a;
            let (b_pid, b_remain_size) = b;
            let test_size = a_remain_size.cmp(b_remain_size);
            match test_size {
                Ordering::Equal => {
                    a_pid.cmp(b_pid)
                }
                _ => test_size
            }
        })
    }

    #[inline]
    fn max_size(&self) -> usize {
        let page_size = self.page_size.get();
        let data_size = page_size - HEADER_SIZE;
        let item_size = data_size / ITEM_SIZE;
        item_size as usize
    }
}
