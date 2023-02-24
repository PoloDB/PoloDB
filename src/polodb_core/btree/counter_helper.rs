use crate::btree::btree_v2::{BTreePageDelegate, BTreePageDelegateWithKey};
use crate::collection_info::CollectionSpecification;
use crate::DbResult;
use crate::session::Session;

pub(crate) fn count(session: &dyn Session, col_spec: &CollectionSpecification) -> DbResult<u64> {
    count_by_btree_pid(session, 0, col_spec.info.root_pid)
}

fn count_by_btree_pid(session: &dyn Session, parent_pid: u32, pid: u32) -> DbResult<u64> {
    let page = session.read_page(pid)?;
    let delegate = BTreePageDelegate::from_page(page.as_ref(), parent_pid)?;
    let btree_content = BTreePageDelegateWithKey::read_from_session(delegate, session)?;
    if btree_content.is_empty() {
        return Ok(0);
    }

    let mut result = btree_content.len() as u64;

    if btree_content.is_leaf() {
        return Ok(result);
    }

    let children_pid = btree_content.children_pid();
    for child_pid in children_pid {
        let child_result = count_by_btree_pid(session, pid, child_pid)?;
        result += child_result;
    }

    Ok(result)
}
