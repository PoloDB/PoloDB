use crate::DbResult;
use crate::bson::Value;
use crate::page::{RawPage, PageHandler};
use super::btree::{BTreeNode, BTreeNodeDataItem, SearchKeyResult};
use super::wrapper_base::BTreePageWrapperBase;
use crate::error::DbErr;

struct DeleteBackwardItem {
    content: BTreeNodeDataItem,
    replace_item: Option<BTreeNodeDataItem>,
    new_pid: Option<u32>,
    child_content_size: usize,
}

pub struct BTreePageDeleteWrapper<'a>(BTreePageWrapperBase<'a>);

impl<'a> BTreePageDeleteWrapper<'a> {

    pub(crate) fn new(page_handler: &mut PageHandler, root_page_id: u32) -> BTreePageDeleteWrapper {
        let base = BTreePageWrapperBase::new(page_handler, root_page_id);
        BTreePageDeleteWrapper(base)
    }

    #[inline]
    pub fn delete_item(&mut self, id: &Value) -> DbResult<bool> {
        self.delete_item_by_pid(0, self.0.root_page_id, id)
    }

    fn delete_item_by_pid(&mut self, parent_pid: u32, pid: u32, id: &Value) -> DbResult<bool> {
        let mut btree_node: BTreeNode = self.0.get_node(pid, parent_pid)?;

        let search_result = btree_node.search(id)?;
        match search_result {
            SearchKeyResult::Index(idx) => {
                let page_id = btree_node.indexes[idx];
                if page_id == 0 {
                    return Ok(false)  // not found
                }

                self.delete_item_by_pid(pid, page_id, id)  // recursiveley delete
            }

            // find the target node
            // use next to replace itself
            // then remove next
            SearchKeyResult::Node(idx) => {
                if btree_node.is_leaf() {
                    let _ = self.delete_item_on_leaf(&mut btree_node, idx)?;
                } else {
                    let backward_item: DeleteBackwardItem = self.delete_next_item(&btree_node, idx)?;
                    self.erase_item(&btree_node.content[idx])?;
                    btree_node.content[idx] = backward_item.content;

                    let mut current_page = RawPage::new(pid, self.0.page_handler.page_size);
                    btree_node.to_raw(&mut current_page)?;

                    self.0.page_handler.pipeline_write_page(&current_page)?;
                }

                Ok(true)  // delete successfully
            }
        }
    }

    fn erase_item(&mut self, item: &BTreeNodeDataItem) -> DbResult<()> {
        if item.overflow_pid == 0 {
            Ok(())
        } else {
            Err(DbErr::NotImplement)
        }
    }

    #[inline]
    fn is_content_size_satisfied(&self, size: usize) -> bool {
        let item_size = self.0.item_size as usize;
        size >= (item_size + 1) / 2 - 1
    }

    #[inline]
    fn get_brothers_id(&self, node_idx: usize) -> (Option<u32>, Option<u32>) {
        let item_size = self.0.item_size as usize;
        if node_idx == 0 {
            (None, Some(1))
        } else if node_idx >= item_size - 1 {
            (Some((node_idx - 1) as u32), None)
        } else {
            (Some((node_idx - 1) as u32), Some((node_idx + 1) as u32))
        }
    }

    // very complex
    fn delete_next_item(&mut self, btree_node: &BTreeNode, node_idx: usize) -> DbResult<DeleteBackwardItem> {
        let next_pid = btree_node.indexes[node_idx + 1];  // get right pid

        let page = self.0.page_handler.pipeline_read_page(next_pid)?;
        let mut child_btree_node = BTreeNode::from_raw(&page, btree_node.pid, self.0.item_size)?;

        if child_btree_node.is_leaf() {
            let backword = self.delete_item_on_leaf(&mut child_btree_node, 0)?;

            if self.is_content_size_satisfied(backword.child_content_size) {  // delete successfully
                return Ok(DeleteBackwardItem {
                    content: backword.content,
                    replace_item: None,
                    new_pid: None,
                    child_content_size: child_btree_node.content.len(),
                });
            }

            let (left_opt, right_opt) = self.get_brothers_id(node_idx);

            let left_node_opt = match left_opt {
                Some(id) => Some(self.0.get_node(id, btree_node.pid)?),
                None => None,
            };
            let right_node_opt = match right_opt {
                Some(id) => Some(self.0.get_node(id, btree_node.pid)?),
                None => None,
            };

            // get max size brother to balance
            let (max_brother_size, is_brother_right) = match (&left_node_opt, &right_node_opt) {
                (Some(node), None) => (node.content.len(), false),
                (None, Some(node)) => (node.content.len(), true),
                (Some(node1), Some(node2)) => {
                    if node1.content.len() < node2.content.len() {
                        (node2.content.len(), false)
                    } else {
                        (node1.content.len(), true)
                    }
                },
                (None, None) => panic!("no brother nodes, pid: {}", page.page_id),
            };

            // if max_brother_size satifies the number, shift one item the middle child
            // if NOT, merge the brother the the middle child
            if self.is_content_size_satisfied(max_brother_size) {
                let replace_item = if is_brother_right { // middle <-(item)- right
                    let mut shift_node = right_node_opt.unwrap();
                    let (_, right_head_content) = shift_node.shift_head();

                    child_btree_node.insert_back(btree_node.content[node_idx].clone(), 0);

                    self.0.write_btree_node(&shift_node)?;
                    self.0.write_btree_node(&child_btree_node)?;

                    right_head_content
                } else {  // left -(item)-> middle
                    let mut shift_node = left_node_opt.unwrap();
                    let (left_last_content, _) = shift_node.shift_last();

                    child_btree_node.insert_head(0, btree_node.content[node_idx].clone());

                    self.0.write_btree_node(&shift_node)?;
                    self.0.write_btree_node(&child_btree_node)?;

                    left_last_content
                };

                Ok(DeleteBackwardItem {
                    content: backword.content,
                    replace_item: Some(replace_item),
                    new_pid: None,
                    child_content_size: btree_node.content.len(),
                })
            } else {  // TODO: merge
                Err(DbErr::NotImplement)
            }
        } else {
            self.delete_next_item(&child_btree_node, 0)  // recursively read next item
            // TODO: handle backword
        }
    }

    fn delete_item_on_leaf(&mut self, btree_node: &mut BTreeNode, index: usize) -> DbResult<DeleteBackwardItem> {
        let result = btree_node.content[index].clone();

        btree_node.content.remove(index);
        btree_node.indexes.remove(index);

        let remain_content_len = btree_node.content.len();

        self.0.write_btree_node(btree_node)?;

        Ok(DeleteBackwardItem {
            content: result,
            replace_item: None,
            new_pid: None,
            child_content_size: remain_content_len,
        })
    }

}
