use crate::DbResult;
use super::wrapper_base::cal_item_size;
use crate::btree::BTreeNode;
use crate::collection_info::CollectionSpecification;
use crate::session::Session;

pub(crate) fn delete_all(session: &dyn Session, col_spec: &CollectionSpecification) -> DbResult<()> {
    let item_size = cal_item_size(session.page_size());
    crate::polo_log!("delete all: {:?}", col_spec);
    delete_all_by_btree_pid(session, item_size, 0, col_spec.info.root_pid)
}

fn delete_all_by_btree_pid(session: &dyn Session, item_size: u32, parent_id: u32, pid: u32) -> DbResult<()> {
    crate::polo_log!("delete all: parent pid: {}, pid: {}", parent_id, pid);
    let page = session.read_page(pid)?;
    let btree_node = BTreeNode::from_raw(&page, parent_id, item_size, session)?;
    if btree_node.content.is_empty() {
        return Ok(())
    }

    for item in btree_node.content {
        session.free_data_ticket(&item.data_ticket)?;
    }

    for child_pid in btree_node.indexes {
        if child_pid == 0 {  // TODO: why?
            continue;
        }
        delete_all_by_btree_pid(session, item_size, pid, child_pid)?;
    }

    session.free_page(pid)?;

    Ok(())
}
