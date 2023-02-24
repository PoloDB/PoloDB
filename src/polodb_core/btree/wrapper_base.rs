use crate::btree::btree_v2::BTreePageDelegateWithKey;
use crate::DbResult;
use crate::session::Session;
use super::btree_v2::BTreePageDelegate;

pub(super) struct BTreePageWrapperBase<'a> {
    pub(super) session:       &'a dyn Session,
    pub(super) root_page_id:       u32,
}

impl<'a> BTreePageWrapperBase<'a> {

    pub(super) fn new(session: &dyn Session, root_page_id: u32) -> BTreePageWrapperBase {
        debug_assert_ne!(root_page_id, 0, "page id is zero");

        BTreePageWrapperBase {
            session,
            root_page_id,
        }
    }

    pub(super) fn get_node(&mut self, pid: u32, parent_pid: u32) -> DbResult<BTreePageDelegateWithKey> {
        let raw_page = self.session.read_page(pid)?;

        let delegate = BTreePageDelegate::from_page(&raw_page, parent_pid)?;
        BTreePageDelegateWithKey::read_from_session(delegate, self.session)
    }

    pub(super) fn write_btree_node(&mut self, btree_page: &BTreePageDelegateWithKey) -> DbResult<()> {
        let raw_page = btree_page.generate_page()?;
        self.session.write_page(&raw_page)
    }

}
