use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) struct TransMap<K, V: Clone> {
    inner: Arc<TransMapInner<K, V>>
}

impl<K, V> TransMap<K, V>
    where V: Clone
{

    pub fn new() -> TransMap<K, V> {
        let inner = TransMapInner::new();
        TransMap {
            inner: Arc::new(inner),
        }
    }

    fn new_with_content(prev: TransMap<K, V>, content: BTreeMap<K, V>) -> TransMap<K, V> {
        let inner = TransMapInner::new_with_content(prev, content);
        TransMap {
            inner: Arc::new(inner),
        }
    }

    pub fn depth(&self) -> usize {
        self.inner.depth
    }

}

impl<K, V> Clone for TransMap<K, V>
    where V: Clone
{

    fn clone(&self) -> TransMap<K, V> {
        TransMap {
            inner: self.inner.clone(),
        }
    }

}

impl<K, V> TransMap<K, V>
    where
        K: Ord,
        V: Clone,
{

    fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

}

struct TransMapInner<K, V: Clone> {
    prev:    Option<TransMap<K, V>>,
    content: BTreeMap<K, V>,
    depth:   usize,
}

impl<K, V> TransMapInner<K, V>
where
    V: Clone
{

    fn new() -> TransMapInner<K, V> {
        TransMapInner {
            prev: None,
            content: BTreeMap::new(),
            depth: 1,
        }
    }

    fn new_with_content(prev: TransMap<K, V>, content: BTreeMap<K, V>) -> TransMapInner<K, V> {
        let prev_depth = prev.depth();
        let prev = Some(prev);
        TransMapInner {
            prev,
            content,
            depth: prev_depth + 1,
        }
    }

}

impl<K, V> TransMapInner<K,V>
    where
        K: Ord,
        V: Clone,
{

    fn get(&self, key: &K) -> Option<&V> {
        match self.content.get(key) {
            Some(v) => Some(v),
            None => {
                match &self.prev {
                    Some(prev) => prev.get(key),
                    None => None
                }
            }
        }
    }

}

pub(crate) struct TransMapDraft<K, V: Clone> {
    base: TransMap<K, V>,
    content: BTreeMap<K, V>,
}

impl<K, V> TransMapDraft<K, V>
    where V: Clone
{

    pub fn new(base: TransMap<K, V>) -> TransMapDraft<K, V> {
        TransMapDraft {
            base,
            content: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V>
        where
            K: Ord,
    {
        self.content.insert(key, value)

    }

    pub fn commit(self) -> TransMap<K, V> {
        let prev = self.base.clone();
        TransMap::new_with_content(prev, self.content)
    }

}

#[cfg(test)]
mod tests {
    use crate::data_structures::trans_map::{TransMap, TransMapDraft};

    #[test]
    fn init() {
        let t0 = TransMap::<i32, i32>::new();
        assert_eq!(t0.depth(), 1);
        let mut draft = TransMapDraft::new(t0.clone());
        draft.insert(1, 1);
        draft.insert(2, 2);
        let t1 = draft.commit();
        assert_eq!(t0.get(&1).map(|r| *r), None);
        assert_eq!(t1.get(&1).map(|r| *r), Some(1));
        assert_eq!(t1.depth(), 2);

        let mut draft = TransMapDraft::new(t1.clone());
        draft.insert(1, 0);
        draft.insert(3, 3);
        let t2 = draft.commit();
        assert_eq!(t2.get(&1).map(|r| *r), Some(0));
        assert_eq!(t2.get(&2).map(|r| *r), Some(2));
        assert_eq!(t2.get(&3).map(|r| *r), Some(3));
        assert_eq!(t2.depth(), 3);
    }

}
