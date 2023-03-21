/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::borrow::Borrow;
use std::cmp::{max, Ordering};
use std::sync::{Arc, RwLock};
use crate::lsm::lsm_tree::tree_cursor::TreeCursor;
use crate::lsm::lsm_tree::value_marker::LsmTreeValueMarker;

const ORDER: usize = 8;

struct DivideInfo<K: Ord + Clone, V: Clone> {
    tuple: (K, LsmTreeValueMarker<V>),
    left: Arc<RwLock<TreeNode<K ,V>>>,
    right: Arc<RwLock<TreeNode<K ,V>>>,
}

impl<K: Ord + Clone, V: Clone> DivideInfo<K, V> {

    fn generate_node(self) -> Arc<RwLock<TreeNode<K, V>>> {
        let mut raw = TreeNode::new();

        raw.data.push(ItemTuple {
            left: Some(self.left),
            key: self.tuple.0,
            value: self.tuple.1,
        });

        raw.right = Some(self.right);

        Arc::new(RwLock::new(raw))
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
    root: Arc<RwLock<TreeNode<K, V>>>,
}

impl<K: Ord + Clone, V: Clone> LsmTree<K, V> {

    pub fn new() -> LsmTree<K, V> {
        let empty = TreeNode::<K, V>::new();
        LsmTree {
            root: Arc::new(RwLock::new(empty)),
        }
    }

    pub fn clear(&mut self) {
        let empty = TreeNode::<K, V>::new();
        self.root = Arc::new(RwLock::new(empty));
    }

    pub fn insert_in_place(&mut self, key: K, value: V) -> Option<V> {
        self.update_in_place(key, LsmTreeValueMarker::Value(value))
    }

    fn update_in_place(&mut self, key: K, value: LsmTreeValueMarker<V>) -> Option<V> {
        let result = {
            let mut root = self.root.write().unwrap();
            root.replace(key, value)
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

    pub fn delete_in_place<Q: ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q> + Ord,
        Q: Ord,
    {
        let mut cursor = self.open_cursor();
        let order = cursor.seek(key);
        if order == Ordering::Equal {
            let old_val = cursor.update_inplace(LsmTreeValueMarker::Deleted);
            old_val.into()
        } else {
            None
        }
    }

    pub fn delete_range_in_place(&mut self, start: &K, end: &K) {
        assert!(start < end);

        self.indeed_delete_range_in_place(start, end);

        self.update_in_place(start.clone(), LsmTreeValueMarker::DeleteStart);
        self.update_in_place(end.clone(), LsmTreeValueMarker::DeleteEnd);
    }

    fn indeed_delete_range_in_place(&mut self, start: &K, end: &K) {
        let mut cursor = self.open_cursor();

        cursor.seek(start);

        while !cursor.done() {
            let key = cursor.key().unwrap();
            if &key > end {
                break;
            }

            cursor.update_inplace(LsmTreeValueMarker::Deleted);

            cursor.next();
        }
    }

    pub fn open_cursor(&self) -> TreeCursor<K, V> {
        TreeCursor::new(self.root.clone())
    }

    pub fn len(&self) -> usize {
        let root_guard = self.root.read().unwrap();
        root_guard.len()
    }

}

#[derive(Clone)]
pub(super) struct ItemTuple<K: Ord + Clone, V: Clone>{
    pub(super) key: K,
    pub(super) value: LsmTreeValueMarker<V>,
    pub(super) left: Option<Arc<RwLock<TreeNode<K, V>>>>,
}

pub(super) struct TreeNode<K: Ord + Clone, V: Clone> {
    pub(super) data: Vec<ItemTuple<K, V>>,
    pub(super) right: Option<Arc<RwLock<TreeNode<K, V>>>>,
}

impl<K: Ord + Clone, V: Clone> TreeNode<K, V> {

    fn new() -> TreeNode<K, V> {
        TreeNode {
            data: Vec::new(),
            right: None,
        }
    }

    fn len(&self) -> usize {
        let mut base = 0;

        for item in &self.data {
            if item.value.is_value() {
                base += 1;
            }
        }

        if self.is_leaf() {
            return base;
        }
        for item in &self.data {
            if let Some(left) = &item.left {
                let left_guard = left.read().unwrap();
                base += left_guard.len();
            }
        }

        if let Some(right) = &self.right {
            let right_guard = right.read().unwrap();
            base += right_guard.len();
        }

        base
    }

    /// Find the index of key
    ///
    /// - `use_greater`: if the 'key' is in the middle between two values,
    ///                  use this flag to determine using greater one
    pub(super) fn find<Q: ?Sized>(&self, key: &Q) -> (usize, Ordering)
    where
        K: Borrow<Q> + Ord,
        Q: Ord,
    {
        assert!(!self.data.is_empty());

        let mut low: isize = 0;
        let mut high: isize = (self.data.len() - 1) as isize;

        while low <= high {
            let middle = (low + high) / 2;
            let tuple = &self.data[middle as usize];

            let cmp_result = key.cmp(tuple.key.borrow());

            match cmp_result {
                Ordering::Equal => {
                    return (middle as usize, cmp_result);
                }

                Ordering::Less => {
                    high = middle - 1;
                }

                Ordering::Greater => {
                    low = middle + 1;
                }
            }

        }

        let idx = max(low, high) as usize;
        if idx >= self.data.len() {
            (idx, Ordering::Greater)
        } else {
            let tuple = &self.data[idx as usize];
            (idx, key.cmp(tuple.key.borrow()))
        }
    }

    #[inline]
    pub(super) fn is_leaf(&self) -> bool {
        self.right.is_none()
    }

    fn insert_in_place(&mut self, key: K, value: V) -> TreeNodeInsertResult<K, V> {
        self.replace(key, LsmTreeValueMarker::Value(value))
    }

    fn replace(&mut self, key: K, value: LsmTreeValueMarker<V>) -> TreeNodeInsertResult<K, V> {
        if self.data.is_empty() {
            self.data.push(ItemTuple {
                key,
                value,
                left: None,
            });
            return TreeNodeInsertResult::Normal;
        }
        let (index, order) = self.find(&key);
        if order == Ordering::Equal {
            let index = index as usize;
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
                    let mut right_page = self.right.as_ref().expect("this is not a leaf").write().unwrap();
                    right_page.replace(key, value)
                } else {
                    let item = &self.data[index];
                    let mut right_page = item.left.as_ref().expect("this is not a leaf").write().unwrap();
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

                        let index = max(0, index) as usize;
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

            Arc::new(RwLock::new(raw))
        };

        let right_node = {
            let mut raw = TreeNode::new();

            for i in (middle_index + 1)..self.data.len() {
                raw.data.push(self.data[i].clone());
            }

            raw.right = self.right.clone();

            Arc::new(RwLock::new(raw))
        };

        let divide_info = DivideInfo {
            tuple,
            left: left_node,
            right: right_node,
        };

        divide_info.into()
    }

}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use crate::lsm::lsm_tree::LsmTree;

    #[test]
    fn test_insert_inplace() {
        let mut tree = LsmTree::new();

        for i in 0..100 {
            tree.insert_in_place(i, i);
        }

        assert_eq!(tree.len(), 100);

        for i in 0..100 {
            let mut cursor = tree.open_cursor();
            cursor.seek(&i);
            assert_eq!(cursor.value().unwrap().unwrap(), i);
        }
    }

    #[test]
    fn test_cursor() {
        let mut tree = LsmTree::new();

        tree.insert_in_place(10, 10);
        tree.insert_in_place(20, 20);
        tree.insert_in_place(30, 30);

        let mut cursor = tree.open_cursor();
        let ord = cursor.seek(&15);

        assert_eq!(ord, Ordering::Less);
        assert_eq!(cursor.value().unwrap().unwrap(), 20);

        let mut cursor = tree.open_cursor();
        let ord = cursor.seek(&5);

        assert_eq!(ord, Ordering::Less);
        assert_eq!(cursor.value().unwrap().unwrap(), 10);

        let mut cursor = tree.open_cursor();
        let ord = cursor.seek(&25);

        assert_eq!(ord, Ordering::Less);
        assert_eq!(cursor.value().unwrap().unwrap(), 30);

        let mut cursor = tree.open_cursor();
        let ord = cursor.seek(&35);

        assert_eq!(ord, Ordering::Greater);
        assert_eq!(cursor.value().unwrap().unwrap(), 30);

        cursor.next();
        assert!(cursor.done());
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
        println!("value: {}", cursor.value().unwrap());
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
            assert_eq!(cursor.key().unwrap(), i);
            cursor.next();
        }

        assert!(cursor.done());
    }

    #[test]
    fn test_delete() {
        let mut tree = LsmTree::new();

        for i in 0..100 {
            tree.insert_in_place(i, i);
        }

        tree.delete_in_place(&50);

        let mut cursor = tree.open_cursor();
        cursor.seek(&50);

        assert!(cursor.value().unwrap().is_deleted())
    }

    #[test]
    fn test_delete_range() {
        let mut tree = LsmTree::new();

        for i in 0..10 {
            tree.insert_in_place(i, i);
        }

        tree.delete_range_in_place(&2, &5);

        let mut cursor = tree.open_cursor();
        cursor.go_to_min();

        assert_eq!(cursor.value().unwrap().unwrap(), 0);
        cursor.next();
        assert_eq!(cursor.value().unwrap().unwrap(), 1);
        cursor.next();
        assert!(cursor.value().unwrap().is_delete_start());  // 2
        cursor.next();
        assert!(cursor.value().unwrap().is_deleted());  // 3
        cursor.next();
        assert!(cursor.value().unwrap().is_deleted());  // 4
        cursor.next();
        assert!(cursor.value().unwrap().is_delete_end());  // 5
        cursor.next();
        assert_eq!(cursor.value().unwrap().unwrap(), 6);

        assert_eq!(tree.len(),  6);
    }

}
