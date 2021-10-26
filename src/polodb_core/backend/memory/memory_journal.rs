use crate::page::RawPage;
use crate::{TransactionType, DbResult};
use std::collections::BTreeMap;

struct JournalFrame {
    page:      RawPage,
    is_commit: bool,
}

pub(super) struct MemoryJournal {
    data:             Vec<JournalFrame>,
    // page_id => data_index
    offset_map:       BTreeMap<u32, usize>,
    transaction_type: Option<TransactionType>,
}

impl MemoryJournal {

    pub(super) fn new() -> MemoryJournal {
        MemoryJournal {
            data:             vec![],
            offset_map:       BTreeMap::new(),
            transaction_type: None,
        }
    }

    pub(super) fn read_page(&self, page_id: u32) -> Option<RawPage> {
        match self.offset_map.get(&page_id) {
            Some(index) => {
                let frame: &JournalFrame = &self.data[*index];
                let page = frame.page.clone();
                Some(page)
            }
            None => None,
        }
    }

    pub(super) fn append_raw_page(&mut self, page: &RawPage) {
        let frame = JournalFrame {
            page: page.clone(),
            is_commit: false,
        };
        self.data.push(frame);
    }

    pub(super) fn commit(&mut self) -> DbResult<()> {
        if let Some(last_frame) = self.data.last_mut() {
            last_frame.is_commit = true;
        }
        self.transaction_type = None;
        Ok(())
    }

    pub(super) fn transaction_type(&self) -> Option<TransactionType> {
        self.transaction_type
    }

    pub(super) fn upgrade_read_transaction_to_write(&mut self) -> DbResult<()> {
        self.transaction_type = Some(TransactionType::Write);
        Ok(())
    }

    pub(super) fn rollback(&mut self) -> DbResult<()> {
        self.rollback_to_last_commit()?;
        self.transaction_type = None;
        Ok(())
    }

    fn rollback_to_last_commit(&mut self) -> DbResult<()> {
        loop {
            let is_commit = match self.data.last() {
                Some(frame) => frame.is_commit,
                None => return Ok(())
            };

            if is_commit {
                return Ok(())
            }

            let _ = self.data.pop();
        }
    }

    pub(super) fn start_transaction(&mut self, ty: TransactionType) -> DbResult<()> {
        self.transaction_type = Some(ty);
        Ok(())
    }

}
