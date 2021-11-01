use std::rc::Rc;
use std::borrow::BorrowMut;
use std::collections::BTreeSet;
use hashbrown::HashMap;
use polodb_bson::{Value, Document};
use super::{BTreeNode, BTreeNodeDataItem, SearchKeyResult};
use super::wrapper_base::BTreePageWrapperBase;
use crate::DbResult;
use crate::page::RawPage;
use crate::page_handler::PageHandler;
use crate::doc_serializer::SerializeType;
use crate::data_ticket::DataTicket;

struct DeleteBackwardItem {
    is_leaf:        bool,
    child_size:     usize,
    deleted_ticket: Box<DataTicket>,
}

pub struct BTreePageDeleteWrapper<'a> {
    base:           BTreePageWrapperBase<'a>,
    dirty_set:      BTreeSet<u32>,
    cache_btree:    HashMap<u32, Box<BTreeNode>>,
    serialize_type: SerializeType,
}

impl<'a> BTreePageDeleteWrapper<'a> {

    pub(crate) fn new(page_handler: &mut PageHandler, root_page_id: u32, serialize_type: SerializeType) -> BTreePageDeleteWrapper {
        let base = BTreePageWrapperBase::new(page_handler, root_page_id);
        BTreePageDeleteWrapper {
            base,
            dirty_set: BTreeSet::new(),
            cache_btree: HashMap::new(),
            serialize_type,
        }
    }

    #[inline]
    fn get_btree_by_pid(&mut self, pid: u32, parent_pid: u32) -> DbResult<Box<BTreeNode>> {
        let node = self.base.get_node(pid, parent_pid)?;
        Ok(Box::new(node))
    }

    // #[inline]
    fn write_btree(&mut self, node: BTreeNode) {
        let mut page = RawPage::new(node.pid, self.base.page_handler.page_size);
        node.to_raw(&mut page).unwrap();
        self.base.page_handler.pipeline_write_page(&page).unwrap();
    }

    pub fn flush_pages(&mut self) -> DbResult<()> {
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
    pub fn delete_item(&mut self, id: &Value) -> DbResult<Option<Rc<Document>>> {
        let backward_item_opt = self.delete_item_on_subtree(0, self.base.root_page_id, id)?;
        match backward_item_opt {
            Some(backward_item) => {
                let item = self.erase_item(backward_item.deleted_ticket.as_ref())?;
                Ok(Some(item))
            }

            None => Ok(None)
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

        if current_btree_node.is_empty() {
            if parent_pid == 0 {  // it's a root node
                return Ok(None);
            }
            panic!("unexpected: node is empty, parent_id={}, pid={}, key={}", parent_pid, pid, id);
        }
        let search_result = current_btree_node.search(id)?;
        match search_result {
            SearchKeyResult::Index(idx) => {
                if current_btree_node.is_leaf() {  // is leaf
                    return Ok(None)  // not found
                }

                let page_id = current_btree_node.indexes[idx];
                let backward_item_opt = self.delete_item_on_subtree(pid, page_id, id)?;  // recursively delete

                if let Some(backward_item) = backward_item_opt {
                    let mut current_item_size = current_btree_node.content.len();

                    if !self.is_content_size_satisfied(backward_item.child_size) {
                        if backward_item.is_leaf {
                            let borrow_ok = self.try_borrow_brothers(idx, current_btree_node.borrow_mut())?;
                            if !borrow_ok {
                                if current_btree_node.content.len() == 1 {
                                    let _opt = self.try_merge_head(*current_btree_node)?;
                                    debug_assert!(_opt);
                                } else {
                                    self.merge_leaves(idx, current_btree_node.borrow_mut())?;
                                    current_item_size = current_btree_node.content.len();
                                    self.write_btree(*current_btree_node);
                                }
                            } else {
                                self.write_btree(*current_btree_node);
                            }
                        } else {
                            // let current_btree_node = self.get_btree_by_pid(pid, parent_pid)?;
                            if current_btree_node.content.len() == 1 {
                                let _opt = self.try_merge_head(*current_btree_node)?;
                                debug_assert!(_opt);
                            }
                        }
                    }
                    return Ok(Some(DeleteBackwardItem {
                        is_leaf: false,
                        child_size: current_item_size,
                        deleted_ticket: backward_item.deleted_ticket,
                    }))
                }

                Ok(None)
            }

            // find the target node
            // use next to replace itself
            // then remove next
            SearchKeyResult::Node(idx) => {
                if current_btree_node.is_leaf() {
                    let backward_item = self.delete_item_on_leaf(current_btree_node, idx)?;
                    Ok(Some(backward_item))
                } else {
                    let deleted_ticket = Box::new(current_btree_node.content[idx].data_ticket.clone());

                    let current_pid = current_btree_node.pid;
                    let subtree_pid = current_btree_node.indexes[idx + 1];
                    let next_item = self.find_min_element_in_subtree(subtree_pid, current_pid)?;
                    current_btree_node.content[idx] = next_item.clone();
                    let mut current_item_size = current_btree_node.content.len();
                    self.write_btree(*current_btree_node);

                    let backward_opt = self.delete_item_on_subtree(current_pid, subtree_pid, &next_item.key)?;
                    match backward_opt {
                        Some(backward_item) => {
                            if !self.is_content_size_satisfied(backward_item.child_size) {
                                if backward_item.is_leaf {  // borrow or merge leaves
                                    let mut current_btree_node = self.get_btree_by_pid(pid, parent_pid)?;
                                    let borrow_ok = self.try_borrow_brothers(idx + 1, current_btree_node.borrow_mut())?;
                                    if !borrow_ok {
                                        // self.merge_leaves(idx + 1, current_btree_node.borrow_mut())?;
                                        // current_item_size = current_btree_node.content.len();

                                        if current_btree_node.content.len() == 1 {
                                            let _opt = self.try_merge_head(*current_btree_node)?;
                                            debug_assert!(_opt);
                                        } else {
                                            self.merge_leaves(idx + 1, current_btree_node.borrow_mut())?;
                                            current_item_size = current_btree_node.content.len();
                                            self.write_btree(*current_btree_node);
                                        }
                                    } else {
                                        self.write_btree(*current_btree_node);
                                    }
                                } else {
                                    let current_btree_node = self.get_btree_by_pid(pid, parent_pid)?;
                                    if current_btree_node.content.len() == 1 {
                                        let _opt = self.try_merge_head(*current_btree_node)?;
                                        debug_assert!(_opt);
                                    }
                                }
                            }

                            Ok(Some(DeleteBackwardItem {
                                is_leaf: false,
                                child_size: current_item_size,
                                deleted_ticket,
                            }))
                        }

                        None => Ok(None),
                    }
                }
            }
        }
    }

    fn try_merge_head(&mut self, parent_btree_node: BTreeNode) -> DbResult<bool> {
        let left_pid = parent_btree_node.indexes[0];
        let right_pid = parent_btree_node.indexes[1];

        let left_node = self.get_btree_by_pid(left_pid, parent_btree_node.pid)?;
        let right_node = self.get_btree_by_pid(right_pid, parent_btree_node.pid)?;

        if left_node.content.len() + right_node.content.len() + 1 > self.base.item_size as usize {
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

        // move
        let new_node: Box<BTreeNode> = Box::new(parent_btree_node.clone_with_contents(new_content, new_indexes));

        self.write_btree(*new_node);

        Ok(true)
    }

    fn try_borrow_brothers(&mut self, node_idx: usize, current_btree_node: &mut BTreeNode) -> DbResult<bool> {
        let current_pid = current_btree_node.pid;

        // node_idx's element on current_btree_node is deleted
        // node on [node_idx] is borrowed
        let subtree_pid = current_btree_node.indexes[node_idx];  // subtree need to shift

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

        // if max_brother_size satisfies the number, shift one item the middle child
        // if NOT, merge the brother the the middle child
        if self.is_content_size_satisfied(max_brother_size) {
            let replace_item = if is_brother_right { // middle <-(item)- right
                let mut shift_node = right_node_opt.unwrap();
                let (_, right_head_content) = shift_node.shift_head();

                subtree_node.insert_back(current_btree_node.content[node_idx].clone(), 0);

                self.write_btree(*shift_node);
                self.write_btree(*subtree_node);

                right_head_content
            } else {  // left -(item)-> middle
                let mut shift_node = left_node_opt.unwrap();
                let (left_last_content, _) = shift_node.shift_last();

                subtree_node.insert_head(0, current_btree_node.content[node_idx].clone());

                self.write_btree(*shift_node);
                self.write_btree(*subtree_node);

                left_last_content
            };

            // shift complete
            current_btree_node.content[node_idx] = replace_item;

            return Ok(true);
        }

        Ok(false)
    }

    // merge the nth elements of the current_btree_node
    fn merge_leaves(&mut self, node_idx: usize, current_btree_node: &mut BTreeNode) -> DbResult<()> {
        debug_assert!(current_btree_node.content.len() > 1);

        let current_pid = current_btree_node.pid;
        let subtree_pid = current_btree_node.indexes[node_idx];  // subtree need to shift

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
                node1.content.len() > node2.content.len(),

            (None, None) => {
                panic!("no brother nodes, pid: {}", subtree_pid)
            },

        };

        let mut subtree_node = self.get_btree_by_pid(subtree_pid, current_pid)?;
        if !is_brother_right {  // left
            let mut left_node = left_node_opt.unwrap();

            left_node.content.push(current_btree_node.content[node_idx - 1].clone());
            left_node.content.extend_from_slice(&subtree_node.content);
            left_node.indexes.extend_from_slice(&subtree_node.indexes);

            current_btree_node.content.remove(node_idx - 1);
            current_btree_node.indexes.remove(node_idx);

            debug_assert_eq!(current_btree_node.indexes[node_idx], subtree_node.pid);

            self.base.page_handler.free_page(subtree_node.pid)?;

            self.write_btree(*left_node);
        } else {  // right
            let right_node = right_node_opt.unwrap();

            subtree_node.content.push(current_btree_node.content[node_idx].clone());
            subtree_node.content.extend_from_slice(&right_node.content);

            subtree_node.indexes.extend_from_slice(&right_node.indexes);

            debug_assert_eq!(current_btree_node.indexes[node_idx + 1], right_node.pid);

            current_btree_node.content.remove(node_idx);
            current_btree_node.indexes.remove(node_idx + 1);

            self.base.page_handler.free_page(right_node.pid)?;

            self.write_btree(*subtree_node);
        }

        Ok(())
    }

    fn erase_item(&mut self, item: &DataTicket) -> DbResult<Rc<Document>> {
        let bytes = self.base.page_handler.free_data_ticket(&item)?;
        debug_assert!(!bytes.is_empty(), "bytes is empty");
        let mut my_ref: &[u8] = bytes.as_ref();
        let doc = crate::doc_serializer::deserialize(self.serialize_type, &mut my_ref)?;
        Ok(Rc::new(doc))
    }

    #[inline]
    fn is_content_size_satisfied(&self, size: usize) -> bool {
        let item_size = self.base.item_size as usize;
        size >= (item_size + 1) / 2 - 1
    }

    fn get_brothers_id(&self, btree_node: &BTreeNode, node_idx: usize) -> (Option<u32>, Option<u32>) {
        if node_idx == 0 {
            let pid = btree_node.indexes[1];
            (None, Some(pid))
        } else if node_idx >= btree_node.indexes.len() - 1 {
            let pid = btree_node.indexes[node_idx - 1];
            (Some(pid), None)
        } else {
            let left_pid = btree_node.indexes[node_idx - 1];
            let right_pid = btree_node.indexes[node_idx + 1];
            (Some(left_pid), Some(right_pid))
        }
    }

    fn delete_item_on_leaf(&mut self, mut btree_node: Box<BTreeNode>, index: usize) -> DbResult<DeleteBackwardItem> {
        let deleted_ticket = Box::new(btree_node.content[index].data_ticket.clone());

        btree_node.content.remove(index);
        btree_node.indexes.remove(index);

        let remain_content_len = btree_node.content.len();

        self.base.write_btree_node(&btree_node)?;

        Ok(DeleteBackwardItem {
            is_leaf: true,
            child_size: remain_content_len,
            deleted_ticket,
        })
    }

}
