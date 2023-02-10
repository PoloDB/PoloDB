use crate::collection_info::CollectionSpecification;
use crate::DbResult;
use crate::session::Session;
use super::BTreeNode;
use super::wrapper_base::cal_item_size;

pub(crate) fn count(session: &dyn Session, col_spec: &CollectionSpecification) -> DbResult<u64> {
    let item_size = cal_item_size(session.page_size());
    count_by_btree_pid(session, item_size, 0, col_spec.info.root_pid)
}

fn count_by_btree_pid(session: &dyn Session, item_size: u32, parent_pid: u32, pid: u32) -> DbResult<u64> {
    let page = session.read_page(pid)?;
    let btree_content = BTreeNode::from_raw(&page, parent_pid, item_size, session)?;
    if btree_content.content.is_empty() {
        return Ok(0)
    }

    let mut result = btree_content.content.len() as u64;

    if btree_content.is_leaf() {
        return Ok(result);
    }

    for child_idx in 0..(btree_content.content.len() + 1) {
        let child_pid = btree_content.indexes[child_idx];
        let child_result = count_by_btree_pid(session, item_size, pid, child_pid)?;
        result += child_result;
    }

    Ok(result)
}
