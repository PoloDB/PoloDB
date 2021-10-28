use std::path::PathBuf;
use std::fs::Metadata;
use std::num::NonZeroU32;
use crate::page::{RawPage, FreeListDataWrapper};
use crate::DbResult;

pub enum PageDump {
    Undefined(u32),
    BTreePage(Box<BTreePageDump>),
    OverflowDataPage(Box<OverflowDataPageDump>),
    DataPage(Box<DataPageDump>),
    FreeListPage(Box<FreeListPageDump>),
}

pub struct FullDump {
    pub identifier:     String,
    pub version:        String,
    pub journal_dump:   Box<JournalDump>,
    pub meta_pid:       u32,
    pub free_list_pid:  u32,
    pub free_list_size: u32,
    pub page_size:      NonZeroU32,
    pub pages:          Vec<PageDump>,
}

pub struct JournalFrameDump {
    pub frame_id:      u32,
    pub db_size:       u64,
    pub salt1:         u32,
    pub salt2:         NonZeroU32,
}

pub struct JournalDump {
    pub path     :   PathBuf,
    pub file_meta:   Metadata,
    pub frame_count: usize,
    pub frames:      Vec<JournalFrameDump>,
}

pub struct BTreePageDump {
    pub pid:       u32,
    pub node_size: usize,
}

impl BTreePageDump {

    #[allow(dead_code)]
    pub(crate) fn from_page(page: &RawPage) -> DbResult<BTreePageDump> {
        Ok(BTreePageDump {
            pid: page.page_id,
            node_size: 0,
        })
    }

}

pub struct OverflowDataPageDump;

pub struct DataPageDump {
    pub pid: u32,
}

impl DataPageDump {

    #[allow(dead_code)]
    pub(crate) fn from_page(page: &RawPage) -> DbResult<DataPageDump> {
        Ok(DataPageDump {
            pid: page.page_id,
        })
    }

}

pub struct FreeListPageDump {
    pub pid: u32,
    pub size: usize,
    pub next_pid: u32
}

impl FreeListPageDump {

    #[allow(dead_code)]
    pub(crate) fn from_page(page: RawPage) -> DbResult<FreeListPageDump> {
        let pid = page.page_id;
        let wrapper = FreeListDataWrapper::from_raw(page);
        Ok(FreeListPageDump {
            pid,
            size: wrapper.size() as usize,
            next_pid: wrapper.next_pid(),
        })
    }

}
