use std::path::PathBuf;
use std::fs::Metadata;
use crate::page::RawPage;
use crate::DbResult;

pub enum PageDump {
    Undefined(u32),
    BTreePage(Box<BTreePageDump>),
    OverflowDataPage(Box<OverflowDataPageDump>),
    DataPage(Box<DataPageDump>),
    FreeListPage(Box<FreeListPageDump>),
}

pub struct FullDump {
    pub path:           PathBuf,
    pub identifier:     String,
    pub version:        String,
    pub file_meta:      Metadata,
    pub journal_dump:   Box<JournalDump>,
    pub meta_pid:       u32,
    pub free_list_pid:  u32,
    pub free_list_size: u32,
    pub page_size:      u32,
    pub pages:          Vec<PageDump>,
}

pub struct JournalDump {
    pub path     : PathBuf,
    pub file_meta:  Metadata,
}

pub struct BTreePageDump {
    pub pid:       u32,
    pub node_size: usize,
}

impl BTreePageDump {

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

    pub(crate) fn from_page(page: &RawPage) -> DbResult<DataPageDump> {
        Ok(DataPageDump {
            pid: page.page_id,
        })
    }

}

pub struct FreeListPageDump;
