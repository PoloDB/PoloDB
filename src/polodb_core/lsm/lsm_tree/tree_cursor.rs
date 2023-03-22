use std::borrow::Borrow;
use std::cmp::{min, Ordering};
use std::sync::{Arc, RwLock};
use smallvec::{SmallVec, smallvec};
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

    pub(crate) fn seek<Q: ?Sized>(&mut self, key: &Q) -> Ordering
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

    fn internal_seek<Q: ?Sized>(&mut self, key: &Q) -> Ordering
    where
        K: Borrow<Q> + Ord,
        Q: Ord
    {
        let back = self.stack.last().expect("the stack is empty").clone();

        let (result, continue_) = {
            let back_guard = back.read().unwrap();
            let (index, order) = back_guard.find(key);
            if order == Ordering::Equal {
                *self.indexes.last_mut().unwrap() = index as usize;
                (order, false)
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
                        (order, true)
                    }
                    None => {
                        let index = min(back_guard.data.len() - 1, index);
                        *self.indexes.last_mut().unwrap() = index;
                        (order, false)
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
        self.stack.last().map(|back| {
            let back_guard = back.read().unwrap();
            let index = *self.indexes.last().unwrap();
            back_guard.data[index].key.clone()
        })
    }

    pub fn value(&self) -> Option<LsmTreeValueMarker<V>> {
        self.stack.last().map(|back| {
            let back_guard = back.read().unwrap();
            let index = *self.indexes.last().unwrap();
            back_guard.data[index].value.clone()
        })
    }

    pub fn marker(&self) -> Option<LsmTreeValueMarker<()>> {
        self.stack.last().map(|back| {
            let back_guard = back.read().unwrap();
            let index = *self.indexes.last().unwrap();
            let value = &back_guard.data[index].value;
            match value {
                LsmTreeValueMarker::Deleted => LsmTreeValueMarker::Deleted,
                LsmTreeValueMarker::DeleteStart => LsmTreeValueMarker::DeleteStart,
                LsmTreeValueMarker::DeleteEnd => LsmTreeValueMarker::DeleteEnd,
                LsmTreeValueMarker::Value(_) => LsmTreeValueMarker::Value(()),
            }
        })
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

}

enum NextDirection<K: Ord + Clone, V: Clone> {
    Leaf(bool),  // is overflow
    Other(Arc<RwLock<TreeNode<K, V>>>),  // next page
}
