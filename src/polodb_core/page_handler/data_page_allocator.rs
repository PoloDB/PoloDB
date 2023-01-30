pub(super) struct DataPageAllocator {
    // pid -> remain size
    free_pages: Vec<(u32, u32)>,
    stash: Option<Vec<(u32, u32)>>,
}

impl DataPageAllocator {

    pub(super) fn new() -> DataPageAllocator {
        DataPageAllocator {
            free_pages: Vec::new(),
            stash: None,
        }
    }

    pub(super) fn add_tuple(&mut self, pid: u32, remain_size: u32) {
        let t = self.stash.as_mut().expect("no transaction");
        t.push((pid, remain_size));
    }

    pub(super) fn try_allocate_data_page(&mut self, need_size: u32) -> Option<(u32, u32)> {
        let t = self.stash.as_mut().expect("no transaction");
        let mut index: i64 = -1;
        let mut result = None;
        for (i, (pid, remain_size)) in t.iter().enumerate() {
            if *remain_size >= need_size {
                index = i as i64;
                result = Some((*pid, *remain_size));
                break;
            }
        }
        if index >= 0 {
            t.remove(index as usize);
        }

        result
    }

    pub(super) fn free_page(&mut self, pid: u32) {
        let t = self.stash.as_mut().unwrap();
        let index_opt = t.iter().position(|(x_pid, _)| *x_pid == pid);
        if let Some(index) = index_opt {
            t.remove(index);
        }
    }

    pub(super) fn start_transaction(&mut self) {
        let copy = self.free_pages.clone();
        self.stash = Some(copy)
    }

    pub(super) fn commit(&mut self) {
        let opt = self.stash.take();
        if let Some(list) = opt {
            self.free_pages = list;
        }
    }

    pub(super) fn rollback(&mut self) {
        self.stash = None;
    }

}
