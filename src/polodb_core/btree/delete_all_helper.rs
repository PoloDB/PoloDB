use crate::DbResult;
use crate::meta_doc_helper::MetaDocEntry;
use crate::page_handler::PageHandler;
use super::wrapper_base::cal_item_size;
use crate::btree::BTreeNode;

pub(crate) fn delete_all(page_handler: &mut PageHandler, collection_meta: MetaDocEntry) -> DbResult<()> {
    let item_size = cal_item_size(page_handler.page_size);
    crate::polo_log!("delete all: {}", collection_meta.doc_ref());
    delete_all_by_btree_pid(page_handler, item_size, 0, collection_meta.root_pid())
}

fn delete_all_by_btree_pid(page_handler: &mut PageHandler, item_size: u32, parent_id: u32, pid: u32) -> DbResult<()> {
    crate::polo_log!("delete all: parent pid: {}, pid: {}", parent_id, pid);
    let page = page_handler.pipeline_read_page(pid)?;
    let btree_node = BTreeNode::from_raw(&page, parent_id, item_size, page_handler)?;
    if btree_node.content.is_empty() {
        return Ok(())
    }

    for item in btree_node.content {
        page_handler.free_data_ticket(&item.data_ticket)?;
    }

    for child_pid in btree_node.indexes {
        if child_pid == 0 {  // TODO: why?
            continue;
        }
        delete_all_by_btree_pid(page_handler, item_size, pid, child_pid)?;
    }

    page_handler.free_page(pid)?;

    Ok(())
}
