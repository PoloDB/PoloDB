use std::ptr;
use std::num::NonZeroU32;
use super::{RawPage, PageType};

const DATA_PAGE_HEADER_SIZE: u32 = 16;

/**
 * Offset 0 (2 bytes): magic number
 *
 * Offset 4 (2 bytes): data len
 * Offset 6 (2 bytes): bar len
 * Offset 8: data begin
 * | 2 bytes | 2 bytes | 2 bytes | 2bytes(zero) |
 */
pub(crate) struct DataPageWrapper {
    page: RawPage,
    remain_size: u32,
}

impl DataPageWrapper {

    pub(crate) fn init(page_id: u32, page_size: NonZeroU32) -> DataPageWrapper {
        let mut raw_page = RawPage::new(page_id, page_size);
        let page_type = PageType::Data;
        raw_page.put(&page_type.to_magic());

        let remain_size = page_size.get() - DATA_PAGE_HEADER_SIZE - 2;

        DataPageWrapper {
            page: raw_page,
            remain_size,
        }
    }

    pub(crate) fn from_raw(raw_page: RawPage) -> DataPageWrapper {
        let bar_len = raw_page.get_u16(6);

        let remain_size = DataPageWrapper::get_remain_size(&raw_page, bar_len as u32);

        DataPageWrapper {
            page: raw_page,
            remain_size,
        }
    }

    fn get_remain_size(raw_page: &RawPage, bar_len: u32) -> u32 {
        if bar_len == 0 {
            raw_page.len() - DATA_PAGE_HEADER_SIZE - 2
        } else {
            let last_bar_index = DATA_PAGE_HEADER_SIZE + (bar_len - 1) * 2;
            let last_bar = raw_page.get_u16(DATA_PAGE_HEADER_SIZE + (bar_len - 1) * 2);
            (last_bar as u32) - last_bar_index - 2
        }
    }

    pub(crate) fn put(&mut self, data: &[u8]) {
        let data_size = data.len() as u32;
        let last_bar = self.get_last_bar();
        let begin_bar: i64 = (last_bar as i64) - (data_size as i64);
        if begin_bar < 0 {
            panic!("data overflow data page size, last_bar: {}, data_size: {}", last_bar, data_size);
        }
        self.page.seek(begin_bar as u32);
        self.page.put(data);

        self.append_bar(begin_bar as u16);

        self.set_bar_len(self.bar_len() + 1);
        self.set_data_len(self.data_len() + 1);

        self.remain_size -= data_size + 2;
    }

    // None representes the item with the index has been removed
    pub(crate) fn get(&self, index: u32) -> Option<&[u8]> {
        if index >= self.bar_len() {
            panic!("index {} is greater than length {}", index, self.bar_len());
        }

        let (begin_bar, end_bar) = self.get_bars_by_index(index);

        if begin_bar == end_bar {
            return None;
        }

        Some(&self.page.data[(begin_bar as usize)..(end_bar as usize)])
    }

    fn get_bars_by_index(&self, index: u32) -> (u16, u16) {
        let begin_bar = self.page.get_u16(DATA_PAGE_HEADER_SIZE + index * 2);
        let end_bar = if index == 0 {
            self.page.len() as u16
        } else {
            self.page.get_u16(DATA_PAGE_HEADER_SIZE + (index - 1) * 2)
        };

        (begin_bar, end_bar)
    }

    fn append_bar(&mut self, bar: u16) {
        let index = DATA_PAGE_HEADER_SIZE + self.bar_len() * 2;
        self.page.seek(index as u32);
        self.page.put_u16(bar);
    }

    // to preserve the index referred by other tickets
    // removing an item will not shift the "bars", and will NOT
    // reduce the len
    //
    // removing an item wll only shift the data
    // the the bar will be equal to the last
    pub(crate) fn remove(&mut self, index: u32) {
        let total_len = self.bar_len();
        if index >= total_len {
            panic!("index {} is greater than length {}", index, self.bar_len());
        }

        let (begin_bar, end_bar) = self.get_bars_by_index(index);

        let item_len = end_bar - begin_bar;

        let last_bar = self.page.get_u16(DATA_PAGE_HEADER_SIZE + (self.bar_len() - 1) * 2);

        let copy_len = begin_bar - last_bar;

        // shift data
        unsafe {
            let buffer_ptr = self.page.data.as_mut_ptr();

            ptr::copy(buffer_ptr.add(last_bar as usize), buffer_ptr.add((last_bar + item_len) as usize), copy_len as usize);
        }

        // set the current bar to ZERO
        self.page.seek(DATA_PAGE_HEADER_SIZE + index * 2);
        self.page.put_u16(if index == 0 {
            self.page.len() as u16
        } else {
            self.get_bar_value(index - 1)
        });

        let mut iter_index = index + 1;
        while iter_index < total_len {
            let bar_index = DATA_PAGE_HEADER_SIZE + iter_index * 2;
            let old_value = self.page.get_u16(bar_index);

            self.page.seek(bar_index);
            self.page.put_u16(old_value + item_len);

            iter_index += 1;
        }

        self.set_data_len(self.data_len() - 1);
        // no need to minus 2bytes for "bar"
        self.remain_size += item_len as u32;
    }

    #[inline]
    fn get_bar_value(&self, index: u32) -> u16 {
        let index = DATA_PAGE_HEADER_SIZE + index * 2;
        self.page.get_u16(index)
    }

    fn get_last_bar(&self) -> u16 {
        if self.bar_len() == 0 {
            return self.page.len() as u16;
        }
        let last_bar_index = DATA_PAGE_HEADER_SIZE + (self.bar_len() - 1) * 2;
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
    pub(crate) fn borrow_page(&self) -> &RawPage {
        &self.page
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn borrow_page_mut(&mut self) -> &mut RawPage {
        &mut self.page
    }

    #[inline]
    pub(crate) fn pid(&self) -> u32 {
        self.page.page_id
    }

    #[inline]
    pub(crate) fn data_len(&self) -> u32 {
        self.page.get_u16(4) as u32
    }

    #[inline]
    pub(crate) fn set_data_len(&mut self, len: u32) {
        self.page.seek(4);
        self.page.put_u16(len as u16);
    }

    #[inline]
    pub(crate) fn bar_len(&self) -> u32 {
        self.page.get_u16(6) as u32
    }

    #[inline]
    pub(crate) fn set_bar_len(&mut self, len: u32) {
        self.page.seek(6);
        self.page.put_u16(len as u16);
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.data_len() == 0
    }

}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use crate::page::data_page_wrapper::DataPageWrapper;

    #[test]
    fn test_put_one_item() {
        let mut wrapper = DataPageWrapper::init(
            1, NonZeroU32::new(4096).unwrap());

        assert_eq!(wrapper.bar_len(), 0);

        let first_item: [u8; 4] = [1, 2, 3, 4];
        wrapper.put(&first_item);

        assert_eq!(wrapper.bar_len(), 1);

        assert_eq!(wrapper.get(0).unwrap(), first_item);

        let raw_page = wrapper.consume_page();
        let wrapper2 = DataPageWrapper::from_raw(raw_page);
        assert_eq!(wrapper2.bar_len(), 1);
        assert_eq!(wrapper2.get(0).unwrap(), first_item);
    }

    #[test]
    fn test_multiple_items() {
        let mut wrapper = DataPageWrapper::init(
            1, NonZeroU32::new(4096).unwrap());

        assert_eq!(wrapper.bar_len(), 0);

        for i in 0..4 {
            let mut first_item: [u8; 4] = [0; 4];
            for j in 0..4 {
                first_item[j] = (i + j) as u8;
            }
            wrapper.put(&first_item);
        }

        assert_eq!(wrapper.bar_len(), 4);

        let raw_page = wrapper.consume_page();
        let wrapper2 = DataPageWrapper::from_raw(raw_page);
        assert_eq!(wrapper2.bar_len(), 4);
    }

    #[test]
    fn test_remove_item() {
        let mut wrapper = DataPageWrapper::init(
            1, NonZeroU32::new(4096).unwrap());

        for i in 0..4 {
            let mut first_item: [u8; 4] = [0; 4];
            for j in 0..4 {
                first_item[j] = (i + j) as u8;
            }
            wrapper.put(&first_item);
        }

        assert_eq!(wrapper.bar_len(), 4);

        wrapper.remove(0);
        assert_eq!(wrapper.data_len(), 3);

        let first = wrapper.get(1).unwrap();
        assert_eq!(first.len(), 4);
        let expected: [u8; 4] = [0, 2, 3, 4];
        for i in 1..4 {
            assert_eq!(first[i], expected[i]);
        }

        wrapper.remove(1);
        assert!(wrapper.get(1).is_none());
    }

}
