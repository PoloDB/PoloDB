use std::collections::BTreeMap;
use std::ops::Bound::{Included, Unbounded};
use crate::page::PageHandler;
use crate::DbResult;
use crate::error::DbErr;
use crate::page::data_page_wrapper::DataPageWrapper;

static DATA_PAGE_HEADER: u32 = 16;

pub(super) struct DataStoreManager {
    page_size: u32,
    map: BTreeMap<u32, Vec<u32>>,
}

impl DataStoreManager {

    pub(super) fn new(page_size: u32) -> DataStoreManager {
        DataStoreManager {
            page_size,
            map: BTreeMap::new(),
        }
    }

    pub(super) fn distribute(&mut self, page_handler: &mut PageHandler, data_size: u32) -> DbResult<DataPageWrapper> {
        let max_allow_size = self.page_size - DATA_PAGE_HEADER - 2;
        if data_size > max_allow_size {
            return Err(DbErr::DataSizeTooLarge(max_allow_size, data_size))
        }

        let mut range = self.map.range_mut((Included(data_size), Unbounded));
        match range.next() {
            Some((_, value)) => {
                let last_index = value[value.len() - 1];
                value.remove(value.len() - 1);

                let raw_page = page_handler.pipeline_read_page(last_index)?;
                let wrapper = DataPageWrapper::from_raw(raw_page);
                Ok(wrapper)
            },
            None => {
                self.distribute_new(page_handler)
            },
        }
    }

    pub(super) fn return_wrapper(&mut self, wrapper: DataPageWrapper) {
        let remain_size = wrapper.remain_size();
        match self.map.get_mut(&remain_size) {
            Some(vector) => {
                vector.push(wrapper.pid());
            }

            None => {
                let vec = vec![ wrapper.pid() ];
                self.map.insert(remain_size, vec);
            }
        }
    }

    fn distribute_new(&mut self, page_handler: &mut PageHandler) -> DbResult<DataPageWrapper> {
        let new_pid = page_handler.alloc_page_id()?;
        let new_wrapper = DataPageWrapper::init(new_pid, page_handler.page_size);
        Ok(new_wrapper)
    }

}
