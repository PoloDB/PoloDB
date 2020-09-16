use crate::DbResult;
use crate::page::{RawPage, PageHandler};
use super::btree::{BTreeNode, HEADER_SIZE, ITEM_SIZE};

pub(super) struct BTreePageWrapperBase<'a> {
    pub(super) page_handler:       &'a mut PageHandler,
    pub(super) root_page_id:       u32,
    pub(super) item_size:          u32,
}

impl<'a> BTreePageWrapperBase<'a> {

    pub(super) fn new(page_handler: &mut PageHandler, root_page_id: u32) -> BTreePageWrapperBase {
        #[cfg(debug_assertions)]
        if root_page_id == 0 {
            panic!("page id is zero");
        }

        let item_size = (page_handler.page_size - HEADER_SIZE) / ITEM_SIZE;

        BTreePageWrapperBase {
            page_handler,
            root_page_id, item_size
        }
    }

    pub(super) fn get_node(&mut self, pid: u32, parent_pid: u32) -> DbResult<BTreeNode> {
        let raw_page = self.page_handler.pipeline_read_page(pid)?;

        BTreeNode::from_raw(&raw_page, parent_pid, self.item_size, self.page_handler)
    }

    pub(super) fn write_btree_node(&mut self, node: &BTreeNode) -> DbResult<()> {
        let mut raw_page = RawPage::new(node.pid, self.page_handler.page_size);

        node.to_raw(&mut raw_page)?;

        self.page_handler.pipeline_write_page(&raw_page)
    }

}
