/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;
use std::sync::{Arc, Mutex};
use smallvec::{SmallVec, smallvec};
use crate::lsm::lsm_tree::value_marker::LsmTreeValueMarker;

const ORDER: usize = 8;

struct DivideInfo<K: Ord + Clone, V: Clone> {
    tuple: (K, LsmTreeValueMarker<V>),
    left: Arc<Mutex<TreeNode<K ,V>>>,
    right: Arc<Mutex<TreeNode<K ,V>>>,
}

impl<K: Ord + Clone, V: Clone> DivideInfo<K, V> {

    fn generate_node(self) -> Arc<Mutex<TreeNode<K, V>>> {
        let mut raw = TreeNode::new();

        raw.data.push(ItemTuple {
            left: Some(self.left),
            key: self.tuple.0,
            value: self.tuple.1,
        });

        raw.right = Some(self.right);

        Arc::new(Mutex::new(raw))
    }

}

enum TreeNodeInsertResult<K: Ord + Clone, V: Clone> {
    Normal,
    LegacyValue(LsmTreeValueMarker<V>),
    Divide(Box<DivideInfo<K, V>>),
}

impl<K: Ord + Clone, V: Clone> From<DivideInfo<K, V>> for TreeNodeInsertResult<K, V> {
    fn from(value: DivideInfo<K, V>) -> Self {
        TreeNodeInsertResult::Divide(Box::new(value))
    }
}

/// This is a simple b-tree implementation.
/// There are several differences between this and the std version:
///
/// 1. Support cursor API
/// 2. Support update in-place and incremental update
/// 3. Does NOT support deletion
#[derive(Clone)]
pub(crate) struct LsmTree<K: Ord + Clone, V: Clone> {
    root: Arc<Mutex<TreeNode<K, V>>>,
}

impl<K: Ord + Clone, V: Clone> LsmTree<K, V> {

    pub fn new() -> LsmTree<K, V> {
        let empty = TreeNode::<K, V>::new();
        LsmTree {
            root: Arc::new(Mutex::new(empty)),
        }
    }

    pub fn insert_in_place(&mut self, key: K, value: V) -> Option<V> {
        let result = {
            let mut root = self.root.lock().unwrap();
            root.insert_in_place(key, value)
        };

        match result {
            TreeNodeInsertResult::Normal => None,
            TreeNodeInsertResult::LegacyValue(val) => val.into(),
            TreeNodeInsertResult::Divide(divide_info) => {
                let node = divide_info.generate_node();

                self.root = node;

                None
            }
        }
    }

    pub fn delete_in_place(&mut self, key: &K) -> Option<V> {
        let mut cursor = self.open_cursor();
        let is_equal = cursor.seek(key);
        if is_equal {
            let old_val = cursor.update_inplace(LsmTreeValueMarker::Deleted);
            old_val.into()
        } else {
            None
        }
    }

    pub fn open_cursor(&self) -> TreeCursor<K, V> {
        TreeCursor::new(self.root.clone())
    }

    pub fn len(&self) -> usize {
        unimplemented!()
    }

}

#[derive(Clone)]
struct ItemTuple<K: Ord + Clone, V: Clone>{
    key: K,
    value: LsmTreeValueMarker<V>,
    left: Option<Arc<Mutex<TreeNode<K, V>>>>,
}

struct TreeNode<K: Ord + Clone, V: Clone> {
    data: Vec<ItemTuple<K, V>>,
    right: Option<Arc<Mutex<TreeNode<K, V>>>>,
}

impl<K: Ord + Clone, V: Clone> TreeNode<K, V> {

    fn new() -> TreeNode<K, V> {
        TreeNode {
            data: Vec::new(),
            right: None,
        }
    }

    fn find(&self, key: &K) -> (usize, bool) {
        if self.data.is_empty() {
            return (0, false);
        }

        let mut low: i32 = 0;
        let mut high: i32 = (self.data.len() - 1) as i32;

        while low <= high {
            let middle = (low + high) / 2;
            let tuple = &self.data[middle as usize];

            let cmp_result = key.cmp(&tuple.key);

            match cmp_result {
                Ordering::Equal => {
                    return (middle as usize, true);
                }

                Ordering::Less => {
                    high = middle - 1;
                }

                Ordering::Greater => {
                    low = middle + 1;
                }
            }

        }

        (std::cmp::max(low, high) as usize, false)
    }

    #[inline]
    fn is_leaf(&self) -> bool {
        self.right.is_none()
    }

    fn insert_in_place(&mut self, key: K, value: V) -> TreeNodeInsertResult<K, V> {
        self.replace(key, LsmTreeValueMarker::Value(value))
    }

    fn replace(&mut self, key: K, value: LsmTreeValueMarker<V>) -> TreeNodeInsertResult<K, V> {
        let (index, is_equal) = self.find(&key);
        if is_equal {
            let prev = self.data[index].value.clone();
            self.data[index].value = value;
            TreeNodeInsertResult::LegacyValue(prev)
        } else {
            if self.is_leaf() {
                let tuple = ItemTuple {
                    key,
                    value,
                    left: None,
                };
                self.data.insert(index, tuple);

                if self.data.len() > ORDER {
                    self.divide_this_node()
                } else {
                    TreeNodeInsertResult::Normal
                }
            } else {
                let insert_result = if index == self.data.len() {
                    let mut right_page = self.right.as_ref().expect("this is not a leaf").lock().unwrap();
                    right_page.replace(key, value)
                } else {
                    let item = &self.data[index];
                    let mut right_page = item.left.as_ref().expect("this is not a leaf").lock().unwrap();
                    right_page.replace(key, value)
                };
                match insert_result {
                    TreeNodeInsertResult::Normal => insert_result,
                    TreeNodeInsertResult::LegacyValue(_) => insert_result,
                    TreeNodeInsertResult::Divide(divide_info) => {
                        let new_item = ItemTuple::<K, V> {
                            key: divide_info.tuple.0,
                            value: divide_info.tuple.1,
                            left: Some(divide_info.left),
                        };

                        self.data.insert(index, new_item);

                        if index == self.data.len() - 1 {  // is last
                            self.right = Some(divide_info.right);
                        } else {
                            self.data[index + 1].left = Some(divide_info.right);
                        }

                        if self.data.len() > ORDER {
                            self.divide_this_node()
                        } else {
                            TreeNodeInsertResult::Normal
                        }
                    }
                }
            }
        }
    }

    fn divide_this_node(&mut self) -> TreeNodeInsertResult<K, V> {
        let middle_index = self.data.len() / 2;
        let tuple = {
            let middle_item = &self.data[middle_index];
            (middle_item.key.clone(), middle_item.value.clone())
        };

        let left_node = {
            let mut raw = TreeNode::new();

            for i in 0..middle_index {
                raw.data.push(self.data[i].clone());
            }

            let middle_item = &self.data[middle_index];
            raw.right = middle_item.left.clone();

            Arc::new(Mutex::new(raw))
        };

        let right_node = {
            let mut raw = TreeNode::new();

            for i in (middle_index + 1)..self.data.len() {
                raw.data.push(self.data[i].clone());
            }

            raw.right = self.right.clone();

            Arc::new(Mutex::new(raw))
        };

        let divide_info = DivideInfo {
            tuple,
            left: left_node,
            right: right_node,
        };

        divide_info.into()
    }

}

pub(crate) struct TreeCursor<K: Ord + Clone, V: Clone> {
    stack: SmallVec<[Arc<Mutex<TreeNode<K, V>>>; 8]>,
    indexes: SmallVec<[usize; 8]>,
    done: bool,
}

impl<K: Ord + Clone, V: Clone> TreeCursor<K, V> {

    fn new(root: Arc<Mutex<TreeNode<K, V>>>) -> TreeCursor<K, V> {
        let result = TreeCursor {
            stack: smallvec![root],
            indexes: smallvec![0],
            done: false,
        };

        // result.go_to_left_most();

        result
    }

    fn go_to_left_most(&mut self) {
        loop {
            let back = self.stack.last().expect("the stack is empty");
            let left = {
                let back_guard = back.lock().unwrap();
                if back_guard.data.is_empty() {
                    break;
                }
                let left_opt = back_guard.data[0].left.clone();
                match left_opt {
                    Some(left) => left,
                    None => {
                        break
                    }
                }
            };
            self.stack.push(left);
            self.indexes.push(0);
        }
    }

    pub(crate) fn seek(&mut self, key: &K) -> bool {
        let back = self.stack.last().expect("the stack is empty").clone();

        let (result, continue_) = {
            let back_guard = back.lock().unwrap();
            let (index, is_equal) = back_guard.find(key);
            if is_equal {
                *self.indexes.last_mut().unwrap() = index;
                (true, false)
            } else {
                let child_opt = if index == back_guard.data.len() {
                    back_guard.right.clone()
                } else {
                    back_guard.data[index].left.clone()
                };
                match child_opt {
                    Some(child) => {
                        self.stack.push(child);
                        *self.indexes.last_mut().unwrap() = index;
                        self.indexes.push(0);
                        (false, true)
                    }
                    None => {
                        *self.indexes.last_mut().unwrap() = index;
                        (false, false)
                    }
                }
            }
        };

        if continue_ {
            self.seek(key)
        } else {
            result
        }
    }

    pub fn key(&self) -> K {
        let back = self.stack.last().expect("the stack is empty");
        let back_guard = back.lock().unwrap();
        let index = *self.indexes.last().unwrap();
        back_guard.data[index].key.clone()
    }

    pub fn value(&self) -> LsmTreeValueMarker<V> {
        let back = self.stack.last().expect("the stack is empty");
        let back_guard = back.lock().unwrap();
        let index = *self.indexes.last().unwrap();
        back_guard.data[index].value.clone()
    }

    pub fn go_to_min(&mut self) {
        self.go_to_left_most();
    }

    pub fn next(&mut self) {
        let direction = {
            let back = self.stack.last().expect("the stack is empty");
            let back_guard = back.lock().unwrap();
            if back_guard.is_leaf() {
                *self.indexes.last_mut().unwrap() += 1;
                let index = *self.indexes.last().unwrap();
                let overflow = index >= back_guard.data.len();  // overflow
                NextDirection::Leaf(overflow)
            } else {
                let index = *self.indexes.last().unwrap();
                let right = if index == back_guard.data.len() - 1 {  // last
                    let right = back_guard.right.as_ref().unwrap().clone();
                    right
                } else {
                    let right = back_guard.data[index + 1].left.as_ref().unwrap().clone();
                    right
                };
                NextDirection::Other(right)
            }
        };

        match direction {
            NextDirection::Leaf(is_overflow) => {
                if is_overflow {
                    self.handle_overflow();
                }
            }
            NextDirection::Other(right_page) => {
                *self.indexes.last_mut().unwrap() += 1;
                self.stack.push(right_page);
                self.indexes.push(0);
                self.go_to_left_most();
            }
        }

    }

    fn handle_overflow(&mut self) {
        while self.stack.len() > 1 {
            self.stack.pop();
            self.indexes.pop();

            let index = *self.indexes.last().unwrap();

            let back = self.stack.last().unwrap();
            let back_guard = back.lock().unwrap();
            if index == back_guard.data.len() {
                if self.stack.len() == 1 {
                    self.done = true;
                    return;
                }
                continue
            } else {
                return;
            }
        }
    }

    #[inline]
    pub fn done(&self) -> bool {
        self.done
    }

    fn update_inplace(&self, value: LsmTreeValueMarker<V>) -> LsmTreeValueMarker<V> {
        let index = *self.indexes.last().unwrap();
        let back = self.stack.last().unwrap();
        let mut back_guard = back.lock().unwrap();
        let old_val = back_guard.data[index].value.clone();
        back_guard.data[index].value = value;
        old_val
    }

}

enum NextDirection<K: Ord + Clone, V: Clone> {
    Leaf(bool),  // is overflow
    Other(Arc<Mutex<TreeNode<K, V>>>),  // next page
}

#[cfg(test)]
mod tests {
    use crate::lsm::lsm_tree::LsmTree;

    #[test]
    fn test_insert_inplace() {
        let mut tree = LsmTree::new();

        for i in 0..100 {
            tree.insert_in_place(i, i);
        }

        for i in 0..100 {
            let mut cursor = tree.open_cursor();
            cursor.seek(&i);
            assert_eq!(cursor.value().unwrap(), i);
        }
    }

    #[test]
    fn test_cursor() {
        let mut tree = LsmTree::new();

        tree.insert_in_place(10, 10);
        tree.insert_in_place(20, 20);
        tree.insert_in_place(30, 30);

        let mut cursor = tree.open_cursor();
        cursor.seek(&15);

        assert_eq!(cursor.value().unwrap(), 20);

        let mut cursor = tree.open_cursor();
        cursor.seek(&5);

        assert_eq!(cursor.value().unwrap(), 10);

        let mut cursor = tree.open_cursor();
        cursor.seek(&25);
        assert_eq!(cursor.value().unwrap(), 30);
    }

    #[test]
    fn test_cursor_2() {
        let mut tree = LsmTree::new();
        for i in 0..16 {
            let i = i * 10;
            tree.insert_in_place(i, i);
        }

        let mut cursor = tree.open_cursor();
        cursor.seek(&15);
        println!("value: {}", cursor.value());
    }

    #[test]
    fn test_next() {
        let mut tree = LsmTree::new();

        for i in 0..100 {
            tree.insert_in_place(i, i);
        }

        let mut cursor = tree.open_cursor();
        cursor.go_to_min();

        for i in 0..100 {
            assert!(!cursor.done());
            assert_eq!(cursor.key(), i);
            cursor.next();
        }

        assert!(cursor.done());
    }

}
