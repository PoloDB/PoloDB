use std::num::NonZeroU32;
use crate::DbResult;
use crate::page::RawPage;
use crate::session::Session;
use super::{BTreeNode, HEADER_SIZE, ITEM_SIZE};

pub(super) struct BTreePageWrapperBase<'a> {
    pub(super) page_handler:       &'a mut dyn Session,
    pub(super) root_page_id:       u32,
    pub(super) item_size:          u32,
}

pub fn cal_item_size(page_size: NonZeroU32) -> u32 {
    (page_size.get() - HEADER_SIZE) / ITEM_SIZE
}

impl<'a> BTreePageWrapperBase<'a> {

    pub(super) fn new(page_handler: &mut dyn Session, root_page_id: u32) -> BTreePageWrapperBase {
        debug_assert_ne!(root_page_id, 0, "page id is zero");

        let item_size = cal_item_size(page_handler.page_size());

        BTreePageWrapperBase {
            page_handler,
            root_page_id,
            item_size
        }
    }

    pub(super) fn get_node(&mut self, pid: u32, parent_pid: u32) -> DbResult<BTreeNode> {
        let raw_page = self.page_handler.pipeline_read_page(pid)?;

        BTreeNode::from_raw(&raw_page, parent_pid, self.item_size, self.page_handler)
    }

    pub(super) fn write_btree_node(&mut self, node: &BTreeNode) -> DbResult<()> {
        let mut raw_page = RawPage::new(node.pid, self.page_handler.page_size());

        node.to_raw(&mut raw_page)?;

        self.page_handler.pipeline_write_page(&raw_page)
    }

}
