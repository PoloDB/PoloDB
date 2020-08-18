use libc::{ malloc, free };
use std::collections::HashMap;
use std::ptr::null_mut;
use crate::page::RawPage;

struct LruNode {
    prev:      *mut LruNode,
    next:      *mut LruNode,
    key:       u32,
    value:     u32,
}

impl LruNode {

    fn new(key: u32, value: u32) -> LruNode {
        LruNode {
            prev: null_mut(),
            next: null_mut(),
            key, value,
        }
    }

}

struct LruMap {
    cap:       usize,
    data:      HashMap<u32, Box<LruNode>>,
    start:     *mut LruNode,
    end:       *mut LruNode,
}

impl LruMap {

    pub fn new(cap: usize) -> LruMap {
        LruMap {
            cap,
            data: HashMap::new(),
            start: null_mut(),
            end: null_mut(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn cap(&self) -> usize {
        self.cap
    }

    pub fn find(&mut self, key: u32) -> Option<u32> {
        let from_map = self.data.remove(&key);
        let node: Box<LruNode> = match from_map {
            Some(own) => own,
            None => return None,
        };

        let result = node.as_ref().value;

        let node: Box<LruNode> = if self.len() > 1 {
            unsafe {
                if node.as_ref().prev.is_null() {  // is head
                    self.data.insert(node.as_ref().key, node); // re insert
                    return Some(result)
                }

                let mut ptr: *mut LruNode = Box::into_raw(node);
                (*(*ptr).prev).next = (*ptr).next; // prev link to next

                if !(*ptr).next.is_null() {  // not a tail
                    (*(*ptr).next).prev = (*ptr).prev;
                } else {
                    self.end = (*ptr).prev;
                }

                (*ptr).prev = null_mut();
                (*ptr).next = self.start;
                (*self.start).prev = ptr;
                self.start = ptr;

                Box::from_raw(ptr)
            }
        } else {
            node
        };

        self.data.insert(node.as_ref().key, node); // re insert
        Some(result)
    }

    pub fn insert(&mut self, key: u32, value: u32) -> Option<u32> {
        let node = LruNode::new(key, value);
        self.insert_node(Box::new(node))
    }

    fn insert_node(&mut self, node: Box<LruNode>) -> Option<u32> {
        let mut result: Option<u32> = None;

        match self.remove(node.as_ref().key) {
            Some(value) => {
                result = Some(value);
            },

            None => {
                if self.len() >= self.cap {
                    match self.remove_tail() {
                        Some((_, value)) =>
                            result = Some(value),

                        None => ()
                    }
                }
            }
        }

        let node = unsafe {
            let box_raw = Box::into_raw(node);

            if self.start.is_null() {  // is head
                self.start = box_raw;
                self.end = box_raw;
            } else {
                (*self.start).prev = box_raw;
                (*box_raw).next = self.start;
                self.start = box_raw;
            }

            Box::from_raw(self.start)
        };

        self.data.insert(node.as_ref().key, node);

        result
    }

    pub fn remove_tail(&mut self) -> Option<(u32, u32)> {
        let len = self.len();
        if len <= 1 {
            self.start = null_mut();
            self.end = null_mut();
            self.data.clear();
            return None;
        }

        let (key, value) = unsafe {
            let tail_node = self.end;
            (*(*self.end).prev).next = null_mut();
            self.end = (*self.end).prev;
            ((*tail_node).key, (*tail_node).value)
        };
        self.data.remove(&key).expect("remove nothing");

        Some((key, value))
    }

    pub fn remove(&mut self, key: u32) -> Option<u32> {
        let ptr: &Box<LruNode> = match self.data.get(&key) {
            Some(node_ref) => node_ref,
            None => return None,
        };

        let len = self.data.len();
        if len <= 1 {
            let value = unsafe {
                let result = (*self.start).value;
                self.start = null_mut();
                self.end = null_mut();
                result
            };
            self.data.clear();
            return Some(value);
        }

        let result = ptr.value;
        unsafe {
            let mut node = &*ptr;

            if node.prev.is_null() {  // head
                (*node.next).prev = null_mut();
                self.start = node.next;
            } else if node.next.is_null() { // tail
                (*node.prev).next = null_mut();
                self.end = node.prev;
            } else {  // middle
                (*node.prev).next = node.next;
                (*node.next).prev = node.prev;
            }

        }
        self.data.remove(&key);

        Some(result)
    }

    pub fn tail(&self) -> Option<(u32, u32)> {
        unsafe {
            if self.end.is_null() {
                return None;
            }

            let ptr = self.end;
            let key = (*ptr).key;
            let value = (*ptr).value;
            Some((key, value))
        }
    }

}

pub(crate) struct PageCache {
    cache_size: usize,
    page_size:  u32,
    data:       *mut u8,
    lru_map:    Box<LruMap>,
}

impl PageCache {

    pub fn new_default(page_size: u32) -> PageCache {
        let four_m = 4 * 1024 * 1024;
        Self::new(four_m, page_size)
    }

    pub fn new(cache_size: usize, page_size: u32) -> PageCache {
        let data: *mut u8 = unsafe {
            malloc(cache_size).cast()
        };

        let page_count = cache_size / (page_size as usize);
        let lru_map = LruMap::new(page_count);

        PageCache {
            cache_size,
            page_size,
            data,
            lru_map: Box::new(lru_map),
        }
    }

    pub fn page_count(&self) -> u32 {
        let result : usize = (self.cache_size) / (self.page_size as usize);
        result as u32
    }

    pub(crate) fn get_from_cache(&mut self, page_id: u32) -> Option<RawPage> {
        let index = match self.lru_map.find(page_id) {
            Some(index) => index,
            None => return None,
        };
        let offset: usize = (index as usize) * (self.page_size as usize);
        let mut result = RawPage::new(page_id, self.page_size);
        unsafe {
            result.copy_from_ptr(self.data.add(offset));
        }
        Some(result)
    }

    pub(crate) fn insert_to_cache(&mut self, page: &RawPage) {
        let new_index = if self.lru_map.len() < self.lru_map.cap() {  // is not full
            self.lru_map.len() as u32
        } else {
            let (_, tail_value) = self.lru_map.remove_tail().expect("data error");
            tail_value
        };

        let offset = (new_index as usize) * (self.page_size as usize);
        unsafe {
            page.copy_to_ptr(self.data.add(offset));
        }

        let _ = self.lru_map.insert(page.page_id, offset as u32);
    }

}

impl Drop for PageCache {

    fn drop (&mut self) {
        unsafe {
            free(self.data.cast())
        }
    }

}

#[cfg(test)]
mod tests {
    use crate::pagecache::LruMap;

    #[test]
    fn lru_map() {
        let mut lru_map = LruMap::new(10);

        for i in 0..100 {
            lru_map.insert(i, i);
        }

        assert_eq!(lru_map.len(), 10);

        for i in 0..90 {
            assert!(lru_map.find(i).is_none());
        }

        for i in 90..100 {
            assert!(lru_map.find(i).is_some());
        }
    }

}
