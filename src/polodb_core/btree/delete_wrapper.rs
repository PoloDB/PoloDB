use std::collections::{BTreeSet, HashMap};
use crate::DbResult;
use crate::bson::Value;
use crate::page::{RawPage, PageHandler};
use super::btree::{BTreeNode, BTreeNodeDataItem, SearchKeyResult};
use super::wrapper_base::BTreePageWrapperBase;
use std::borrow::BorrowMut;
use crate::error::DbErr;

struct DeleteBackwardItem {
    is_leaf: bool,
    child_size: usize,
}

pub struct BTreePageDeleteWrapper<'a> {
    base: BTreePageWrapperBase<'a>,
    dirty_set: BTreeSet<u32>,
    cache_btree: HashMap<u32, Box<BTreeNode>>,
}

impl<'a> BTreePageDeleteWrapper<'a> {

    pub(crate) fn new(page_handler: &mut PageHandler, root_page_id: u32) -> BTreePageDeleteWrapper {
        let base = BTreePageWrapperBase::new(page_handler, root_page_id);
        BTreePageDeleteWrapper {
            base,
            dirty_set: BTreeSet::new(),
            cache_btree: HashMap::new(),
        }
    }

    fn get_btree_by_pid(&mut self, pid: u32, parent_pid: u32) -> DbResult<Box<BTreeNode>> {
        match self.cache_btree.remove(&pid) {
            Some(node) => {
                self.dirty_set.remove(&pid);
                Ok(node)
            }

            None => {
                let node = self.base.get_node(pid, parent_pid)?;
                Ok(Box::new(node))
            }

        }
    }

    #[inline]
    fn write_btree(&mut self, node: Box<BTreeNode>) {
        self.dirty_set.insert(node.pid);
        self.cache_btree.insert(node.pid, node);
    }

    fn flush_pages(&mut self) -> DbResult<()> {
        for pid in &self.dirty_set {
            let node = self.cache_btree.remove(pid).unwrap();
            let mut page = RawPage::new(node.pid, self.base.page_handler.page_size);
            node.to_raw(&mut page)?;

            self.base.page_handler.pipeline_write_page(&page)?;
        }

        self.dirty_set.clear();

        Ok(())
    }

    // case 1: item to be deleted on leaf
    // case 2: NOT on leaf
    //         - replace it with item on leaf
    //         - delete item on leaf
    pub fn delete_item(&mut self, id: &Value) -> DbResult<bool> {
        let mut root_btree_node: Box<BTreeNode> = self.get_btree_by_pid(self.base.root_page_id, 0)?;

        let search_result = root_btree_node.search(id)?;
        match search_result {
            SearchKeyResult::Index(idx) => {  // delete item in subtree
                let subtree_pid = root_btree_node.indexes[idx];
                match self.delete_item_on_subtree(root_btree_node.pid, subtree_pid, id)? {
                    Some(_) => Ok(true),
                    None => Ok(false)
                }
            }

            SearchKeyResult::Node(idx) => {
                self.erase_item(&root_btree_node.content[idx])?;

                if root_btree_node.is_leaf() {
                    let _ = self.delete_item_on_leaf(root_btree_node, idx)?;
                    self.flush_pages()?;
                    return Ok(true)
                }

                let current_pid = root_btree_node.pid;
                let next_pid = root_btree_node.indexes[idx + 1];
                let next_item = self.find_min_element_in_subtree(next_pid, current_pid)?;

                root_btree_node.content[idx] = next_item.clone();
                self.write_btree(root_btree_node);

                match self.delete_item_on_subtree(current_pid, next_pid, &next_item.key)? {
                    Some(_) => Ok(true),
                    None => Ok(false)
                }
            }

        }
    }

    fn find_min_element_in_subtree(&mut self, subtree_pid: u32, parent_pid: u32) -> DbResult<BTreeNodeDataItem> {
        let btree_node = self.get_btree_by_pid(subtree_pid, parent_pid)?;
        if btree_node.is_leaf() {
            let first = btree_node.content[0].clone();
            Ok(first)
        } else {
            let next_pid = btree_node.indexes[0];
            self.find_min_element_in_subtree(next_pid, subtree_pid)
        }
    }

    fn delete_item_on_subtree(&mut self, parent_pid: u32, pid: u32, id: &Value) -> DbResult<Option<DeleteBackwardItem>> {
        let mut current_btree_node: Box<BTreeNode> = self.get_btree_by_pid(pid, parent_pid)?;

        let search_result = current_btree_node.search(id)?;
        match search_result {
            SearchKeyResult::Index(idx) => {
                if current_btree_node.is_leaf() {  // is leaf
                    return Ok(None)  // not found
                }

                let page_id = current_btree_node.indexes[idx];
                self.delete_item_on_subtree(pid, page_id, id)  // recursively delete
            }

            // find the target node
            // use next to replace itself
            // then remove next
            SearchKeyResult::Node(idx) => {
                self.erase_item(&current_btree_node.content[idx])?;
                if current_btree_node.is_leaf() {
                    self.delete_item_on_leaf(current_btree_node, idx)
                } else {
                    let current_pid = current_btree_node.pid;
                    let subtree_pid = current_btree_node.indexes[idx + 1];
                    let next_item = self.find_min_element_in_subtree(subtree_pid, current_pid)?;
                    current_btree_node.content[idx] = next_item.clone();
                    let mut current_item_size = current_btree_node.content.len();
                    self.write_btree(current_btree_node);

                    let backward_opt = self.delete_item_on_subtree(current_pid, subtree_pid, &next_item.key)?;
                    return match backward_opt {
                        Some(backward_item) => {
                            if !self.is_content_size_satisfied(backward_item.child_size) {
                                if backward_item.is_leaf {  // borrow or merge leaves
                                    let mut current_btree_node = self.get_btree_by_pid(pid, parent_pid)?;
                                    let borrow_ok = self.try_borrow_brothers(idx, current_btree_node.borrow_mut())?;
                                    if !borrow_ok {
                                        self.merge_leaves(idx, current_btree_node.borrow_mut())?;
                                        current_item_size = current_btree_node.content.len();
                                    }
                                    self.write_btree(current_btree_node);
                                } else {
                                    let current_btree_node = self.get_btree_by_pid(pid, parent_pid)?;
                                    if current_btree_node.content.len() == 1 {
                                        let _opt = self.try_merge_head(current_btree_node)?;
                                    }
                                }
                            }

                            Ok(Some(DeleteBackwardItem {
                                is_leaf: false,
                                child_size: current_item_size,
                            }))
                        }

                        None => Ok(None),
                    }
                }
            }
        }
    }

    fn try_merge_head(&mut self, parent_btree_node: Box<BTreeNode>) -> DbResult<bool> {
        let left_pid = parent_btree_node.indexes[0];
        let right_pid = parent_btree_node.indexes[1];

        let left_node = self.get_btree_by_pid(left_pid, parent_btree_node.pid)?;
        let right_node = self.get_btree_by_pid(right_pid, parent_btree_node.pid)?;

        if left_node.content.len() + right_node.content.len() + 1 > self.base.item_size as usize {
            self.write_btree(left_node);
            self.write_btree(right_node);
            return Ok(false);
        }

        let mut new_content = Vec::with_capacity(self.base.item_size as usize);
        let mut new_indexes = Vec::with_capacity((self.base.item_size + 1) as usize);

        new_content.extend_from_slice(&left_node.content);
        new_indexes.extend_from_slice(&left_node.indexes);

        new_content.push(parent_btree_node.content[0].clone());

        new_content.extend_from_slice(&right_node.content);
        new_indexes.extend_from_slice(&right_node.indexes);

        self.base.page_handler.free_pages(&[left_pid, right_pid])?;

        self.write_btree(parent_btree_node);

        Ok(true)
    }

    fn try_borrow_brothers(&mut self, node_idx: usize, current_btree_node: &mut BTreeNode) -> DbResult<bool> {
        let current_pid = current_btree_node.pid;
        let subtree_pid = current_btree_node.indexes[node_idx + 1];  // subtree need to shift

        let (left_opt, right_opt) = self.get_brothers_id(&current_btree_node, node_idx);

        let left_node_opt = match left_opt {
            Some(pid) => Some(self.get_btree_by_pid(pid, current_pid)?),
            None => None,
        };
        let right_node_opt = match right_opt {
            Some(pid) => Some(self.get_btree_by_pid(pid, current_pid)?),
            None => None,
        };

        // get max size brother to balance
        let (max_brother_size, is_brother_right) = match (&left_node_opt, &right_node_opt) {
            (Some(node), None) => (node.content.len(), false),
            (None, Some(node)) => (node.content.len(), true),
            (Some(node1), Some(node2)) => {
                if node1.content.len() < node2.content.len() {
                    (node2.content.len(), true)
                } else {
                    (node1.content.len(), false)
                }
            },
            (None, None) => {
                panic!("no brother nodes, pid: {}", subtree_pid)
            },
        };

        let mut subtree_node = self.get_btree_by_pid(subtree_pid, current_pid)?;

        // if max_brother_size satifies the number, shift one item the middle child
        // if NOT, merge the brother the the middle child
        if self.is_content_size_satisfied(max_brother_size) {
            let replace_item = if is_brother_right { // middle <-(item)- right
                let mut shift_node = right_node_opt.unwrap();
                let (_, right_head_content) = shift_node.shift_head();

                subtree_node.insert_back(current_btree_node.content[node_idx].clone(), 0);

                self.write_btree(shift_node);
                self.write_btree(subtree_node);

                right_head_content
            } else {  // left -(item)-> middle
                let mut shift_node = left_node_opt.unwrap();
                let (left_last_content, _) = shift_node.shift_last();

                subtree_node.insert_head(0, current_btree_node.content[node_idx].clone());

                self.write_btree(shift_node);
                self.write_btree(subtree_node);

                left_last_content
            };

            // shift complete
            current_btree_node.content[node_idx] = replace_item;

            return Ok(true);
        }

        Ok(false)
    }

    fn merge_leaves(&mut self, node_idx: usize, current_btree_node: &mut BTreeNode) -> DbResult<()> {
        let current_pid = current_btree_node.pid;
        let subtree_pid = current_btree_node.indexes[node_idx + 1];  // subtree need to shift

        let (left_opt, right_opt) = self.get_brothers_id(&current_btree_node, node_idx);

        let left_node_opt = match left_opt {
            Some(pid) => Some(self.get_btree_by_pid(pid, current_pid)?),
            None => None,
        };
        let right_node_opt = match right_opt {
            Some(pid) => Some(self.get_btree_by_pid(pid, current_pid)?),
            None => None,
        };

        // get min size brother to merge
        let is_brother_right = match (&left_node_opt, &right_node_opt) {
            (Some(_), None) => false,
            (None, Some(_)) => true,
            (Some(node1), Some(node2)) => {
                if node1.content.len() <= node2.content.len() {
                    false
                } else {
                    true
                }
            },
            (None, None) => {
                panic!("no brother nodes, pid: {}", subtree_pid)
            },
        };

        let mut subtree_node = self.get_btree_by_pid(subtree_pid, current_pid)?;
        if !is_brother_right {  // left
            let mut left_node = left_node_opt.unwrap();

            left_node.content.push(current_btree_node.content[node_idx].clone());
            left_node.content.extend_from_slice(&subtree_node.content);
            while left_node.indexes.len() != left_node.content.len() + 1 {  // fill zero
                left_node.indexes.push(0);
            }

            current_btree_node.content.remove(node_idx);
            current_btree_node.indexes.remove(node_idx + 1);

            self.base.page_handler.free_page(subtree_node.pid)?;

            self.write_btree(left_node);
        } else {  // right
            let right_node = right_node_opt.unwrap();

            subtree_node.content.push(current_btree_node.content[node_idx + 1].clone());
            subtree_node.content.extend_from_slice(&right_node.content);
            while subtree_node.indexes.len() != subtree_node.content.len() + 1 {  // fill zero
                subtree_node.indexes.push(0);
            }

            current_btree_node.content.remove(node_idx + 1);
            current_btree_node.indexes.remove(node_idx + 2);

            self.base.page_handler.free_page(right_node.pid)?;

            self.write_btree(subtree_node);
        }

        Ok(())
    }

    fn erase_item(&mut self, _item: &BTreeNodeDataItem) -> DbResult<()> {
        return Err(DbErr::NotImplement)
    }

    #[inline]
    fn is_content_size_satisfied(&self, size: usize) -> bool {
        let item_size = self.base.item_size as usize;
        size >= (item_size + 1) / 2 - 1
    }

    #[inline]
    fn get_brothers_id(&self, btree_node: &BTreeNode, node_idx: usize) -> (Option<u32>, Option<u32>) {
        let item_size = self.base.item_size as usize;
        if node_idx == 0 {
            let pid = btree_node.indexes[1];
            (None, Some(pid))
        } else if node_idx >= item_size - 1 {
            let pid = btree_node.indexes[node_idx - 1];
            (Some(pid), None)
        } else {
            let left_pid = btree_node.indexes[node_idx - 1];
            let right_pid = btree_node.indexes[node_idx + 1];
            (Some(left_pid), Some(right_pid))
        }
    }

    fn delete_item_on_leaf(&mut self, mut btree_node: Box<BTreeNode>, index: usize) -> DbResult<Option<DeleteBackwardItem>> {
        // let result = btree_node.content[index].clone();

        btree_node.content.remove(index);
        btree_node.indexes.remove(index);

        let remain_content_len = btree_node.content.len();

        self.base.write_btree_node(&btree_node)?;

        Ok(Some(DeleteBackwardItem {
            is_leaf: true,
            child_size: remain_content_len,
        }))
    }

}
