use std::num::NonZeroU32;
use std::sync::Arc;
use crate::data_structures::trans_map::{TransMap, TransMapDraft};
use crate::page::RawPage;

#[derive(Clone)]
pub(crate) struct DbSnapshot {
    page_map: TransMap<u32, Arc<RawPage>>,
    page_size: NonZeroU32,
    db_file_size: u64,
}

impl DbSnapshot {

    pub fn new(page_size: NonZeroU32, db_file_size: u64) -> DbSnapshot {
        DbSnapshot {
            page_map: TransMap::new(),
            page_size,
            db_file_size
        }
    }

    pub fn read_page(&self, page_id: u32) -> Option<RawPage> {
        self.page_map
            .get(&page_id)
            .map(|page_ref| page_ref.as_ref().clone())
    }

    #[inline]
    pub fn db_file_size(&self) -> u64 {
        self.db_file_size
    }
}

pub(crate) struct DbSnapshotDraft {
    base: DbSnapshot,
    page_map_draft: TransMapDraft<u32, Arc<RawPage>>,
    db_file_size: u64,
}

impl DbSnapshotDraft {

    pub fn new(base: DbSnapshot) -> DbSnapshotDraft {
        let db_file_size = base.db_file_size;
        let page_map_draft = TransMapDraft::new(base.page_map.clone());
        DbSnapshotDraft {
            base,
            page_map_draft,
            db_file_size,
        }
    }

    pub fn commit(self) -> DbSnapshot {
        let db_file_size = self.db_file_size;
        let page_size = self.base.page_size;
        DbSnapshot {
            page_map: self.page_map_draft.commit(),
            page_size,
            db_file_size,
        }
    }

    pub fn read_page(&self, page_id: u32) -> Option<RawPage> {
        self.page_map_draft
            .get(&page_id)
            .map(|page_ref| page_ref.as_ref().clone())
    }

    pub fn write_page(&mut self, page: &RawPage) {
        let new_page = Arc::new(page.clone());
        self.page_map_draft.insert(page.page_id, new_page);
    }

    #[inline]
    pub fn db_file_size(&self) -> u64 {
        self.db_file_size
    }

    #[inline]
    pub fn set_db_file_size(&mut self, file_size: u64) {
        self.db_file_size = file_size;
    }
}
