use std::borrow::Borrow;
use std::cmp::{min, Ordering};
use std::sync::{Arc, RwLock};
use smallvec::{SmallVec, smallvec};
use crate::lsm::lsm_tree::LsmTree;
use super::lsm_tree::TreeNode;
use super::LsmTreeValueMarker;

pub(crate) struct TreeCursor<K: Ord + Clone, V: Clone> {
    root: Arc<RwLock<TreeNode<K, V>>>,
    stack: SmallVec<[Arc<RwLock<TreeNode<K, V>>>; 8]>,
    indexes: SmallVec<[usize; 8]>,
}

impl<K: Ord + Clone, V: Clone> TreeCursor<K, V> {

    pub(super) fn new(root: Arc<RwLock<TreeNode<K, V>>>) -> TreeCursor<K, V> {
        let result = TreeCursor {
            root,
            stack: smallvec![],
            indexes: smallvec![],
        };

        result
    }

    fn go_to_left_most(&mut self) {
        loop {
            let back = self.stack.last().expect("the stack is empty");
            let left = {
                let back_guard = back.read().unwrap();
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

    pub(crate) fn seek<Q: ?Sized>(&mut self, key: &Q) -> Option<Ordering>
    where
        K: Borrow<Q> + Ord,
        Q: Ord
    {
        self.stack.clear();
        self.indexes.clear();

        self.stack.push(self.root.clone());
        self.indexes.push(0);

        self.internal_seek(key)
    }

    fn internal_seek<Q: ?Sized>(&mut self, key: &Q) -> Option<Ordering>
    where
        K: Borrow<Q> + Ord,
        Q: Ord
    {
        let back = self.stack.last().expect("the stack is empty").clone();

        let (result, continue_) = {
            let back_guard = back.read().unwrap();
            if back_guard.data.is_empty() {
                return None;
            }
            let (index, order) = back_guard.find(key);
            if order == Ordering::Equal {
                *self.indexes.last_mut().unwrap() = index as usize;
                (Some(order), false)
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
                        (Some(order), true)
                    }
                    None => {
                        let index = min(back_guard.data.len() - 1, index);
                        *self.indexes.last_mut().unwrap() = index;
                        (Some(order), false)
                    }
                }
            }
        };

        if continue_ {
            self.internal_seek(key)
        } else {
            result
        }
    }

    pub fn key(&self) -> Option<K> {
        self.stack
            .last()
            .map(|back| {
                let back_guard = back.read().unwrap();
                let index = *self.indexes.last().unwrap();
                if back_guard.data.is_empty() {
                    None
                } else {
                    Some(back_guard.data[index].key.clone())
                }
            })
            .flatten()
    }

    pub fn value(&self) -> Option<LsmTreeValueMarker<V>> {
        self.stack
            .last()
            .map(|back| {
                let back_guard = back.read().unwrap();
                let index = *self.indexes.last().unwrap();
                if back_guard.data.is_empty() {
                    None
                } else {
                    Some(back_guard.data[index].value.clone())
                }
            })
            .flatten()
    }

    pub fn marker(&self) -> Option<LsmTreeValueMarker<()>> {
        self.stack.last()
            .map(|back| {
                let back_guard = back.read().unwrap();
                if back_guard.data.is_empty() {
                    return None
                }
                let index = *self.indexes.last().unwrap();
                let value = &back_guard.data[index].value;
                Some(match value {
                    LsmTreeValueMarker::Deleted => LsmTreeValueMarker::Deleted,
                    LsmTreeValueMarker::DeleteStart => LsmTreeValueMarker::DeleteStart,
                    LsmTreeValueMarker::DeleteEnd => LsmTreeValueMarker::DeleteEnd,
                    LsmTreeValueMarker::Value(_) => LsmTreeValueMarker::Value(()),
                })
            })
            .flatten()
    }

    pub fn tuple(&self) -> Option<(K, LsmTreeValueMarker<V>)> {
        self.stack.last().map(|back| {
            let back_guard = back.read().unwrap();
            let index = *self.indexes.last().unwrap();
            let item = &back_guard.data[index];
            (item.key.clone(), item.value.clone())
        })
    }

    pub fn go_to_min(&mut self) {
        self.stack.push(self.root.clone());
        self.indexes.push(0);
        self.go_to_left_most();
    }

    pub fn next(&mut self) {
        let direction = {
            let back = self.stack.last().expect("the stack is empty");
            let back_guard = back.read().unwrap();
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
        while self.stack.len() > 0 {
            self.stack.pop();
            self.indexes.pop();

            if self.stack.is_empty() {
                return;
            }

            let back = self.stack.last().unwrap();
            let index = *self.indexes.last().unwrap();

            let back_guard = back.read().unwrap();
            if index == back_guard.data.len() {
                continue
            } else {
                return;
            }
        }
    }

    #[inline]
    pub fn done(&self) -> bool {
        self.stack.is_empty()
    }

    pub(super) fn update_inplace(&self, value: LsmTreeValueMarker<V>) -> LsmTreeValueMarker<V> {
        let index = *self.indexes.last().unwrap();
        let back = self.stack.last().unwrap();
        let mut back_guard = back.write().unwrap();
        let old_val = back_guard.data[index].value.clone();
        back_guard.data[index].value = value;
        old_val
    }

    pub(crate) fn update(&mut self, value: &LsmTreeValueMarker<V>) -> Option<(LsmTree<K, V>, Option<V>)> {
        let stack_len = self.stack.len() as i64;
        if stack_len == 0 {
            return None;
        }
        let mut legacy: Option<LsmTreeValueMarker<V>> = None;
        let mut index = stack_len - 1;

        while index >= 0 {
            let node = self.stack[index as usize].clone();
            let data_index = self.indexes[index as usize];
            let node_reader = node.read().unwrap();

            if node_reader.data.is_empty() {
                assert_eq!(index, 0);
                return None;
            }

            let mut cloned = node_reader.clone();

            if index == stack_len - 1 {
                legacy = Some(cloned.data[data_index].value.clone());
                cloned.data[data_index].value = value.clone();
            } else {
                let next = self.stack[(index + 1) as usize].clone();
                if data_index == cloned.data.len() {
                    cloned.right = Some(next);
                } else {
                    cloned.data[data_index].left = Some(next);
                }
            }
            self.stack[index as usize] = Arc::new(RwLock::new(cloned));

            index -= 1;
        }

        let result = (
            LsmTree::new_with_root(self.stack[0].clone()),
            legacy.unwrap().into(),
        );
        Some(result)
    }

    pub(crate) fn insert(&mut self, key: K, value: &LsmTreeValueMarker<V>) -> LsmTree<K, V> {
        let root_node_ref = self.root.clone();
        let root_tree = LsmTree::<K, V>::new_with_root(root_node_ref);
        let new_tree = root_tree.update(key, value.clone());
        new_tree
    }

    pub(crate) fn reset(&mut self) {
        self.stack.clear();
        self.indexes.clear();
    }

}

enum NextDirection<K: Ord + Clone, V: Clone> {
    Leaf(bool),  // is overflow
    Other(Arc<RwLock<TreeNode<K, V>>>),  // next page
}
