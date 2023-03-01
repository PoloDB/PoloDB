use std::collections::BTreeSet;
use hashbrown::HashMap;
use bson::{Bson, Document};
use crate::btree::btree_v2::{BTreeDataItemWithKey, BTreePageDelegateWithKey};
use super::SearchKeyResult;
use super::wrapper_base::BTreePageWrapperBase;
use crate::DbResult;
use crate::data_ticket::DataTicket;
use crate::session::Session;

struct DeletedContent {
    key_ticket: Option<DataTicket>,
    payload: DataTicket,
}

impl DeletedContent {

    fn from_data_item(item: &BTreeDataItemWithKey) -> DeletedContent {
        DeletedContent {
            key_ticket: item.key_data_ticket.clone(),
            payload: item.payload.clone(),
        }
    }

}

struct DeleteBackwardItem {
    #[allow(dead_code)]
    is_leaf:           bool,
    child_remain_size: i32,
    deleted_content:   DeletedContent,
}

pub struct BTreePageDeleteWrapper<'a> {
    base:           BTreePageWrapperBase<'a>,
    dirty_set:      BTreeSet<u32>,
    cache_btree:    HashMap<u32, BTreePageDelegateWithKey>,
}

impl<'a> BTreePageDeleteWrapper<'a>  {

    pub(crate) fn new(session: &dyn Session, root_page_id: u32) -> BTreePageDeleteWrapper {
        let base = BTreePageWrapperBase::new(session, root_page_id);
        BTreePageDeleteWrapper {
            base,
            dirty_set: BTreeSet::new(),
            cache_btree: HashMap::new(),
        }
    }

    #[inline]
    fn get_btree_by_pid(&mut self, pid: u32, parent_pid: u32) -> DbResult<BTreePageDelegateWithKey> {
        self.base.get_node(pid, parent_pid)
    }

    fn write_btree(&mut self, node: &BTreePageDelegateWithKey) -> DbResult<()> {
        let page = node.generate_page()?;
        self.base.session.write_page(&page)
    }

    pub fn flush_pages(&mut self) -> DbResult<()> {
        for pid in &self.dirty_set {
            let node = self.cache_btree.remove(pid).unwrap();
            let page = node.generate_page()?;
            self.base.session.write_page(&page)?;
        }

        self.dirty_set.clear();

        Ok(())
    }

    // case 1: item to be deleted on leaf
    // case 2: NOT on leaf
    //         - replace it with item on leaf
    //         - delete item on leaf
    pub fn delete_item(&mut self, id: &Bson) -> DbResult<Option<Document>> {
        let backward_item_opt = self.delete_item_on_subtree(self.base.root_page_id, 0, id)?;
        match backward_item_opt {
            Some(backward_item) => {
                let item = self.erase_item(&backward_item.deleted_content)?;
                Ok(Some(item))
            }

            None => Ok(None)
        }
    }

    fn find_min_element_in_subtree(&mut self, subtree_pid: u32, parent_pid: u32) -> DbResult<BTreeDataItemWithKey> {
        let btree_node = self.get_btree_by_pid(subtree_pid, parent_pid)?;
        if btree_node.is_leaf() {
            let first = btree_node.get_item(0).clone();
            Ok(first)
        } else {
            let next_pid = btree_node.get_item(0).left_pid;
            self.find_min_element_in_subtree(next_pid, subtree_pid)
        }
    }

    fn delete_item_on_subtree(&mut self, pid: u32, parent_pid: u32, id: &Bson) -> DbResult<Option<DeleteBackwardItem>> {
        let mut current_btree_node = self.get_btree_by_pid(pid, parent_pid)?;

        if current_btree_node.is_empty() {
            if parent_pid == 0 {  // it's a root node
                return Ok(None);
            }
            panic!("unexpected: node is empty, parent_id={}, pid={}, key={}", parent_pid, pid, id);
        }
        let search_result = current_btree_node.search(id)?;
        match search_result {
            SearchKeyResult::Index(idx) => {  // The node to delete is on the left of `idx`
                if current_btree_node.is_leaf() {
                    // There is nothing on the left, so nothing is deleted
                    return Ok(None);
                }
                let page_id = current_btree_node.get_left_pid(idx);
                let backward_item_opt = self.delete_item_on_subtree(page_id, pid, id)?;  // recursively delete

                if backward_item_opt.is_none() {
                    // Nothing is deleted, return out
                    return Ok(None);
                }

                // Something is deleted on the left branch,
                // the b-tree needs to be re-balance.
                let backward_item = backward_item_opt.unwrap();

                if BTreePageDeleteWrapper::remain_size_too_large(&current_btree_node, backward_item.child_remain_size) {
                    return self.re_balance_left_branch(current_btree_node, backward_item, idx);
                }

                Ok(Some(DeleteBackwardItem {
                    is_leaf: false,
                    child_remain_size: current_btree_node.remain_size(),
                    deleted_content: backward_item.deleted_content,
                }))
            },
            SearchKeyResult::Node(idx) => {
                if current_btree_node.is_leaf() {
                    let backward_item = self.delete_item_on_leaf(current_btree_node, idx)?;
                    return Ok(Some(backward_item));
                }

                let deleted_content = DeletedContent::from_data_item(current_btree_node.get_item(idx));

                let current_pid = current_btree_node.page_id();
                let subtree_pid = current_btree_node.get_right_pid(idx);
                let next_item = self.find_min_element_in_subtree(subtree_pid, current_pid)?;
                current_btree_node.update_content(idx, next_item.clone());
                self.write_btree(&current_btree_node)?;

                let backward_opt = self.delete_item_on_subtree(
                    subtree_pid, current_pid, &next_item.key,
                )?;

                if backward_opt.is_none() {
                    return Ok(Some(DeleteBackwardItem {
                        is_leaf: false,
                        child_remain_size: current_btree_node.remain_size(),
                        deleted_content,
                    }));
                }

                let backward_item = backward_opt.unwrap();

                if BTreePageDeleteWrapper::remain_size_too_large(&current_btree_node, backward_item.child_remain_size) {
                    return self.re_balance_left_branch(current_btree_node, backward_item, idx + 1);
                    // if backward_item.is_leaf {  // borrow or merge leaves
                    //     let borrow_ok = self.try_borrow_brothers(idx + 1, current_btree_node.borrow_mut())?;
                    //     if !borrow_ok {
                    //         // self.merge_leaves(idx + 1, current_btree_node.borrow_mut())?;
                    //         // current_item_size = current_btree_node.content.len();
                    //
                    //         if current_btree_node.len() == 1 {
                    //             let head_opt = self.try_merge_head(current_btree_node)?;
                    //             let head = head_opt.unwrap();
                    //             return Ok(Some(DeleteBackwardItem {
                    //                 is_leaf: true,
                    //                 child_remain_size: head.remain_size(),
                    //                 deleted_content,
                    //             }))
                    //         } else {
                    //             self.merge_leaves(idx + 1, current_btree_node.borrow_mut())?;
                    //             self.write_btree(&current_btree_node)?;
                    //         }
                    //     } else {
                    //         self.write_btree(&current_btree_node)?;
                    //     }
                    // } else {
                    //     let current_btree_node = self.get_btree_by_pid(pid, parent_pid)?;
                    //     if current_btree_node.len() == 1 {
                    //         let head_opt = self.try_merge_head(current_btree_node)?;
                    //         let head = head_opt.unwrap();
                    //         return Ok(Some(DeleteBackwardItem {
                    //             is_leaf: false,
                    //             child_remain_size: head.remain_size(),
                    //             deleted_content,
                    //         }));
                    //     }
                    // }
                }

                Ok(Some(DeleteBackwardItem {
                    is_leaf: false,
                    child_remain_size: current_btree_node.remain_size(),
                    deleted_content,
                }))
            },
        }
    }

    /// Condition: current branch is not a leaf
    fn re_balance_left_branch(&mut self, mut current_btree_node: BTreePageDelegateWithKey, backward_item: DeleteBackwardItem, idx: usize) -> DbResult<Option<DeleteBackwardItem>> {
        if current_btree_node.parent_id() == 0 && current_btree_node.len() == 1 {
            // merge the children
            let new_node_opt = self.try_merge_head(current_btree_node)?;
            let new_node = new_node_opt.unwrap();
            return Ok(Some(DeleteBackwardItem {
                is_leaf: false,
                child_remain_size: new_node.remain_size(),
                deleted_content: backward_item.deleted_content,
            }));
        }

        // if we reach here, it's saying we deleted an item on the leaf
        // and this node is a parent of a leaf.
        // We can check if it's possible to merge this leaf with other leaves
        let borrow_ok = self.try_borrow_brothers(idx, &mut current_btree_node)?;

        if borrow_ok {
            self.write_btree(&current_btree_node)?;
        } else if current_btree_node.len() == 1 {
            let new_node_opt = self.try_merge_head(current_btree_node)?;
            let new_node = new_node_opt.unwrap();
            return Ok(Some(DeleteBackwardItem {
                is_leaf: true,
                child_remain_size: new_node.remain_size(),
                deleted_content: backward_item.deleted_content,
            }));
        } else {
            // merge leaves
            self.merge_leaves(idx, &mut current_btree_node)?;
            self.write_btree(&current_btree_node)?;
            return Ok(Some(DeleteBackwardItem {
                is_leaf: false,
                child_remain_size: current_btree_node.remain_size(),
                deleted_content: backward_item.deleted_content,
            }));
        };

        Ok(Some(DeleteBackwardItem {
            is_leaf: false,
            child_remain_size: current_btree_node.remain_size(),
            deleted_content: backward_item.deleted_content,
        }))
    }

    fn remain_size_too_large(btree_node: &BTreePageDelegateWithKey, remain_size: i32) -> bool {
        let storage_size = btree_node.storage_size();
        let quarter = (storage_size * 3 / 4) as i32;
        remain_size > quarter
    }

    fn try_merge_head(&mut self, parent_btree_node: BTreePageDelegateWithKey) -> DbResult<Option<BTreePageDelegateWithKey>> {
        let left_pid = parent_btree_node.get_left_pid(0);
        let right_pid = parent_btree_node.get_right_pid(0);

        let left_node = self.get_btree_by_pid(left_pid, parent_btree_node.page_id())?;
        let right_node = self.get_btree_by_pid(right_pid, parent_btree_node.page_id())?;

        let one_item_byte_size = parent_btree_node.get_item(0).bytes_size();

        if left_node.bytes_size() + right_node.bytes_size() + one_item_byte_size + 2 > parent_btree_node.storage_size() as i32 {
            return Ok(None);
        }

        let center_node = parent_btree_node.get_item(0).clone();
        // move
        let new_node = BTreePageDelegateWithKey::merge_with_center(
            parent_btree_node.page_id(),
            parent_btree_node.parent_id(),
            parent_btree_node.page_size(),
            &left_node,
            &right_node,
            center_node,
        )?;
        self.base.session.free_pages(&[left_pid, right_pid])?;

        self.write_btree(&new_node)?;

        Ok(Some(new_node))
    }

    /// If a node needs to borrows, it's saying that
    /// The remain size of this node is too large.
    ///
    /// node_idx: is the left index of current_node
    fn try_borrow_brothers(&mut self, node_idx: usize, current_btree_node: &mut BTreePageDelegateWithKey) -> DbResult<bool> {
        let current_pid = current_btree_node.page_id();

        // node_idx's element on current_btree_node is deleted
        // node on [node_idx] is borrowed
        let subtree_pid = current_btree_node.get_left_pid(node_idx); // subtree need to shift

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
        // The bigger node remain size should be smaller
        let (bigger_node_remain_size, is_right) = match (&left_node_opt, &right_node_opt) {
            (Some(node), None) => (node.remain_size(), false),
            (None, Some(node)) => (node.remain_size(), true),
            (Some(node1), Some(node2)) => {
                if node1.bytes_size() < node2.bytes_size() {
                    (node2.remain_size(), true)
                } else {
                    (node1.remain_size(), false)
                }
            },
            (None, None) => {
                panic!("no brother nodes, pid: {}", subtree_pid)
            },
        };

        let mut subtree_node = self.get_btree_by_pid(subtree_pid, current_pid)?;

        // If max_brother_size satisfies the number, shift one item the middle child.
        // Otherwise, merge the brother the the middle child
        // if !BTreePageDeleteWrapper::remain_size_too_large(current_btree_node, max_brother_size) {
        let replace_item = if is_right { // middle <-(item)- right
            let mut shift_node = right_node_opt.unwrap();
            let mut right_item = shift_node.shift_head();
            let shift_node_bytes_size = right_item.bytes_size();

            // If this brother becomes too small after borrowing the node,
            // the balancing is not worth. Give up and return.
            if BTreePageDeleteWrapper::remain_size_too_large(current_btree_node, bigger_node_remain_size + shift_node_bytes_size + 2) {
                return Ok(false);
            }

            let middle_item = current_btree_node.get_item(node_idx).clone();
            let original_left_pid_of_right = right_item.left_pid;  // remember the original left
            right_item.left_pid = middle_item.left_pid;  // point to the subtree node

            subtree_node.insert_back(middle_item, original_left_pid_of_right);

            self.write_btree(&shift_node)?;
            self.write_btree(&subtree_node)?;

            right_item
        } else {  // left -(item)-> middle
            let mut shift_node = left_node_opt.unwrap();
            let (mut left_last_content, last_right_pid) = shift_node.shift_last();
            let shift_node_bytes_size = left_last_content.bytes_size();

            // If this brother becomes too small after borrowing the node,
            // the balancing is not worth. Give up and return.
            if BTreePageDeleteWrapper::remain_size_too_large(current_btree_node, bigger_node_remain_size + shift_node_bytes_size + 2) {
                return Ok(false);
            }

            let mut middle_item = current_btree_node.get_item(node_idx).clone();
            left_last_content.left_pid = middle_item.left_pid;
            middle_item.left_pid = last_right_pid;
            subtree_node.insert_head(middle_item);

            self.write_btree(&shift_node)?;
            self.write_btree(&subtree_node)?;

            left_last_content
        };

        // shift complete
        current_btree_node.update_content(node_idx, replace_item);

        return Ok(true);
    }

    // merge the nth elements of the current_btree_node
    fn merge_leaves(&mut self, node_idx: usize, current_btree_node: &mut BTreePageDelegateWithKey) -> DbResult<()> {
        assert!(current_btree_node.len() > 1);

        let current_pid = current_btree_node.page_id();
        let subtree_pid = current_btree_node.get_left_pid(node_idx);  // subtree need to shift

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

            (Some(node1), Some(node2)) =>
                node1.bytes_size() > node2.bytes_size(),

            (None, None) => panic!("no brother nodes, pid: {}", subtree_pid),
        };

        let mut subtree_node = self.get_btree_by_pid(subtree_pid, current_pid)?;
        if !is_brother_right {  // left
            let mut left_node = left_node_opt.unwrap();

            left_node.push(current_btree_node.get_item(node_idx - 1).clone());
            left_node.merge_left_leave(&subtree_node);

            current_btree_node.get_item_mut(node_idx).left_pid = left_node.page_id();
            current_btree_node.remove_item(node_idx - 1);

            self.base.session.free_page(subtree_pid)?;

            self.write_btree(&left_node)?;
        } else {  // right
            let right_node = right_node_opt.unwrap();

            subtree_node.push(current_btree_node.get_item(node_idx).clone());
            subtree_node.merge_left_leave(&right_node);

            // subtree_node.content.push(current_btree_node.content[node_idx].clone());
            // subtree_node.content.extend_from_slice(&right_node.content);
            //
            // subtree_node.indexes.extend_from_slice(&right_node.indexes);
            //
            // assert_eq!(current_btree_node.indexes[node_idx + 1], right_node.pid);

            current_btree_node.remove_item(node_idx);
            // set left pid for the new node
            current_btree_node.get_item_mut(node_idx).left_pid = subtree_pid;

            self.base.session.free_page(right_node.page_id())?;

            self.write_btree(&subtree_node)?;
        }

        Ok(())
    }

    fn erase_item(&mut self, item: &DeletedContent) -> DbResult<Document> {
        if let Some(key_ticket) = &item.key_ticket {
            self.base.session.free_data_ticket(key_ticket)?;
        }
        let bytes = self.base.session.free_data_ticket(&item.payload)?;
        assert!(!bytes.is_empty(), "bytes is empty");
        let mut my_ref: &[u8] = bytes.as_ref();
        let doc = crate::doc_serializer::deserialize(&mut my_ref)?;
        Ok(doc)
    }

    // #[inline]
    // fn is_content_size_satisfied(&self, size: usize) -> bool {
    //     let item_size = self.base.item_size as usize;
    //     size >= (item_size + 1) / 2 - 1
    // }

    /// node_idx: is the left pid
    fn get_brothers_id(&self, btree_node: &BTreePageDelegateWithKey, node_idx: usize) -> (Option<u32>, Option<u32>) {
        if node_idx == 0 {
            let pid = btree_node.get_right_pid(node_idx);
            (None, Some(pid))
        } else if node_idx == btree_node.len() {
            let pid = btree_node.get_left_pid(node_idx - 1);
            (Some(pid), None)
        } else {
            let left_pid = btree_node.get_left_pid(node_idx - 1);
            let right_pid = btree_node.get_right_pid(node_idx + 1);
            (Some(left_pid), Some(right_pid))
        }
    }

    fn delete_item_on_leaf(&mut self, mut btree_node: BTreePageDelegateWithKey, index: usize) -> DbResult<DeleteBackwardItem> {
        let deleted_content = DeletedContent::from_data_item(btree_node.get_item(index));

        btree_node.remove_item(index);

        let remain_size = btree_node.remain_size();

        self.base.write_btree_node(&btree_node)?;

        Ok(DeleteBackwardItem {
            is_leaf: true,
            child_remain_size: remain_size,
            deleted_content,
        })
    }

    pub(crate) fn indeed_delete_item_on_btree(session: &dyn Session, item: &BTreeDataItemWithKey) -> DbResult<()> {
        if let Some(key_data_ticket) = &item.key_data_ticket {
            session.free_data_ticket(key_data_ticket)?;
        }
        session.free_data_ticket(&item.payload)?;
        Ok(())
    }

}
