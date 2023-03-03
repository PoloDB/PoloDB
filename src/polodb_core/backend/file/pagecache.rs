use std::num::{NonZeroU32, NonZeroUsize};
use lru::LruCache;
use std::alloc::{alloc, dealloc, Layout};
use std::sync::{Arc, Mutex};
use crate::page::RawPage;

pub(crate) struct PageCache {
    inner: Mutex<PageCacheInner>,
}

impl PageCache {
    #[allow(dead_code)]
    pub fn new(page_count: usize, page_size: NonZeroU32) -> PageCache {
        let inner = PageCacheInner::new(page_count, page_size);
        PageCache {
            inner: Mutex::new(inner),
        }
    }

    pub fn new_default(page_size: NonZeroU32) -> PageCache {
        let inner = PageCacheInner::new_default(page_size);
        PageCache {
            inner: Mutex::new(inner),
        }
    }

    pub fn get_from_cache(&self, page_id: u32) -> Option<Arc<RawPage>> {
        let mut inner = self.inner.lock().unwrap();
        inner.get_from_cache(page_id)
    }

    pub fn insert_to_cache(&self, page: &RawPage) {
        let mut inner = self.inner.lock().unwrap();
        inner.insert_to_cache(page)
    }

}

struct PageCacheInner {
    page_count: usize,
    page_size:  NonZeroU32,
    layout:     Layout,
    data:       *mut u8,
    lru_map:    LruCache<u32, u32>,
}

unsafe impl Send for PageCacheInner {}

impl PageCacheInner {

    fn new_default(page_size: NonZeroU32) -> PageCacheInner {
        Self::new(1024, page_size)
    }

    fn new(page_count: usize, page_size: NonZeroU32) -> PageCacheInner {
        let cache_size = page_count * (page_size.get() as usize);

        let layout = Layout::from_size_align(cache_size, 8).unwrap();
        let data: *mut u8 = unsafe {
            alloc(layout.clone()).cast()
        };

        PageCacheInner {
            page_count,
            page_size,
            layout,
            data,
            lru_map: LruCache::new(NonZeroUsize::new(page_count).unwrap()),
        }
    }

    fn get_from_cache(&mut self, page_id: u32) -> Option<Arc<RawPage>> {
        let index = match self.lru_map.get(&page_id) {
            Some(index) => index,
            None => return None,
        };
        let offset: usize = (*index as usize) * (self.page_size.get() as usize);
        let mut result = RawPage::new(page_id, self.page_size);
        unsafe {
            result.copy_from_ptr(self.data.add(offset as usize));
        }
        Some(Arc::new(result))
    }

    #[inline]
    fn distribute_new_index(&mut self) -> u32 {
        if self.lru_map.len() < self.page_count {  // is not full
            self.lru_map.len() as u32
        } else {
            let (_, tail_value) = self.lru_map.pop_lru().expect("data error");
            tail_value
        }
    }

    fn insert_to_cache(&mut self, page: &RawPage) {
        match self.lru_map.get(&page.page_id) {
            Some(index) => {  // override
                let offset = (*index as usize) * (self.page_size.get() as usize);
                unsafe {
                    page.copy_to_ptr(self.data.add(offset));
                }
            }

            None => {
                let index = self.distribute_new_index();
                let offset = (index as usize) * (self.page_size.get() as usize);
                unsafe {
                    page.copy_to_ptr(self.data.add(offset));
                }
                let _ = self.lru_map.put(page.page_id, index);
            },
        };
    }

}

impl Drop for PageCacheInner {

    fn drop (&mut self) {
        let layout = self.layout.clone();
        unsafe {
            dealloc(self.data.cast(), layout)
        };
    }

}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use crate::backend::file::pagecache::PageCache;
    use crate::page::RawPage;

    fn make_raw_page(page_id: u32) -> RawPage {
        let mut page = RawPage::new(page_id, NonZeroU32::new(4096).unwrap());

        for i in 0..4096 {
            page.data[i] = unsafe {
                libc::rand() as u8
            }
        }

        page
    }

    static TEST_PAGE_LEN: u32 = 10;

    #[test]
    fn page_cache() {
        let page_cache = PageCache::new(3, NonZeroU32::new(4096).unwrap());

        let mut ten_pages = Vec::with_capacity(TEST_PAGE_LEN as usize);

        for i in 0..TEST_PAGE_LEN {
            ten_pages.push(make_raw_page(i))
        }

        for i in 0..3 {
            page_cache.insert_to_cache(&ten_pages[i as usize]);
        }

        for i in 0..3 {
            let page = page_cache.get_from_cache(i).unwrap();

            for (index, ch) in page.data.iter().enumerate() {
                assert_eq!(*ch, ten_pages[i as usize].data[index])
            }
        }


        for i in 3..6 {
            page_cache.insert_to_cache(&ten_pages[i as usize]);
        }

        for i in 0..3 {
            if let Some(_) = page_cache.get_from_cache(i) {
                panic!("removed");
            };
        }

        for i in 3..6 {
            let page = page_cache.get_from_cache(i).unwrap();

            for (index, ch) in page.data.iter().enumerate() {
                assert_eq!(*ch, ten_pages[i as usize].data[index])
            }
        }
    }

}
