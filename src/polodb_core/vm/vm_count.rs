use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use bson::{Bson, Document};
use crate::vm::vm_external_func::{VmExternalFunc, VmExternalFuncStatus};
use crate::Result;

const NAME: &'static str = "count";

pub(crate) struct VmFuncCount {
    count_name: String,
    pub(crate) count: AtomicU64,
    is_completed: AtomicBool,
}

impl VmFuncCount {
    pub(crate) fn new(count_name: String) -> VmFuncCount {
        VmFuncCount {
            count_name,
            count: AtomicU64::new(0),
            is_completed: AtomicBool::new(false),
        }
    }

}

impl VmExternalFunc for VmFuncCount {
    fn name(&self) -> &str {
        NAME
    }
    fn call(&self, args: &[Bson]) -> Result<VmExternalFuncStatus> {
        if args.len() == 0 {  // complete
            self.is_completed.store(true, Ordering::Relaxed);
            let mut doc = Document::new();
            doc.insert(self.count_name.clone(), self.count.load(Ordering::Relaxed) as i64);
            return Ok(VmExternalFuncStatus::Next(doc.into()));
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(VmExternalFuncStatus::Continue)
    }

    fn is_completed(&self) -> bool {
        self.is_completed.load(Ordering::Relaxed)
    }

}
