/**
 * Offset 0 (2 bytes): magic number
 *
 * Offset 8: data begin
 * | 2 bytes | 2 bytes | 2 bytes | 2bytes(zero) |
 */
use super::page::{RawPage, PageType};
use std::cell::Cell;

static DATA_PAGE_HEADER_SIZE: u32 = 16;
static DATA_INDEX_END_PADDING_SIZE: u32 = 2;

pub(crate) struct DataPageWrapper {
    page: RawPage,
    data_len: u32,
    remain_size: u32,
}

impl DataPageWrapper {

    pub(crate) fn init(page_id: u32, page_size: u32) -> DataPageWrapper {
        let mut raw_page = RawPage::new(page_id, page_size);
        let page_type = PageType::Data;
        raw_page.put(&page_type.to_magic());

        let remain_size = page_size - DATA_PAGE_HEADER_SIZE - 2 - DATA_INDEX_END_PADDING_SIZE;

        DataPageWrapper {
            page: raw_page,
            data_len: 0,
            remain_size,
        }
    }

    pub(crate) fn from_raw(raw_page: RawPage) -> DataPageWrapper {
        let mut data_len = Cell::new(0);
        let mut remain_size = Cell::new(0);

        DataPageWrapper::get_data_len_and_remain_size(&raw_page, &mut data_len, &mut remain_size);

        DataPageWrapper {
            page: raw_page,
            data_len: data_len.get(),
            remain_size: remain_size.get(),
        }
    }

    fn get_data_len_and_remain_size(raw_page: &RawPage, data_len: &mut Cell<u32>, remain_size: &mut Cell<u32>) {
        remain_size.set(raw_page.len() - DATA_PAGE_HEADER_SIZE - DATA_INDEX_END_PADDING_SIZE);

        let mut bar_index = DATA_PAGE_HEADER_SIZE;

        loop {
            let begin_bar = raw_page.get_u16(bar_index);
            if begin_bar == 0 {
                break;
            }

            data_len.set(data_len.get() + 1);
            remain_size.set((begin_bar as u32) - bar_index - DATA_INDEX_END_PADDING_SIZE - 2);

            bar_index += 2;
        }
    }

    pub(crate) fn put(&mut self, data: &[u8]) {
        let data_size = data.len() as u32;
        let last_bar = self.get_last_bar();
        let begin_bar = (last_bar as u32) - data_size;
        self.page.seek(begin_bar as u32);
        self.page.put(data);

        self.append_bar(begin_bar as u16);

        self.remain_size -= data_size;
    }

    pub(crate) fn get(&self, index: u32) -> &[u8] {
        if index >= self.data_len {
            panic!("index {} is greater than length {}", index, self.data_len);
        }

        let begin_bar = self.page.get_u16(DATA_PAGE_HEADER_SIZE + index * 2);
        let end_bar = if index == 0 {
            self.page.len() as u16
        } else {
            self.page.get_u16(DATA_PAGE_HEADER_SIZE + (index - 1) * 2)
        };

        &self.page.data[(begin_bar as usize)..(end_bar as usize)]
    }

    fn append_bar(&mut self, bar: u16) {
        let index = DATA_PAGE_HEADER_SIZE + self.data_len * 2;
        self.page.seek(index as u32);
        self.page.put_u16(bar);
        self.data_len += 1;
    }

    fn get_last_bar(&self) -> u16 {
        if self.data_len == 0 {
            return self.page.len() as u16;
        }
        let last_bar_index = DATA_PAGE_HEADER_SIZE + (self.data_len - 1) * 2;
        self.page.get_u16(last_bar_index as u32)
    }

    #[inline]
    pub(crate) fn remain_size(&self) -> u32 {
        self.remain_size
    }

    #[inline]
    pub(crate) fn consume_page(self) -> RawPage {
        self.page
    }

    #[inline]
    pub(crate) fn pid(&self) -> u32 {
        self.page.page_id
    }

    #[inline]
    pub(crate) fn len(&self) -> u32 {
        self.data_len
    }

}

#[cfg(test)]
mod tests {
    use crate::page::data_page_wrapper::DataPageWrapper;

    #[test]
    fn test_put_one_item() {
        let mut wrapper = DataPageWrapper::init(1, 4096);

        assert_eq!(wrapper.len(), 0);

        let first_item: [u8; 4] = [1, 2, 3, 4];
        wrapper.put(&first_item);

        assert_eq!(wrapper.len(), 1);

        assert_eq!(wrapper.get(0), first_item);

        let raw_page = wrapper.consume_page();
        let wrapper2 = DataPageWrapper::from_raw(raw_page);
        assert_eq!(wrapper2.len(), 1);
        assert_eq!(wrapper2.get(0), first_item);
    }

    #[test]
    fn test_multiple_items() {
        let mut wrapper = DataPageWrapper::init(1, 4096);

        assert_eq!(wrapper.len(), 0);

        for i in 0..4 {
            let mut first_item: [u8; 4] = [0; 4];
            for j in 0..4 {
                first_item[j] = (i + j) as u8;
            }
            wrapper.put(&first_item);
        }

        assert_eq!(wrapper.len(), 4);

        let raw_page = wrapper.consume_page();
        let wrapper2 = DataPageWrapper::from_raw(raw_page);
        assert_eq!(wrapper2.len(), 4);
    }

}
