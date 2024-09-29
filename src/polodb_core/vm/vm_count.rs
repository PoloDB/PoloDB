// Copyright 2024 Vincent Chan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use bson::{Bson, Document};
use crate::vm::vm_external_func::{VmExternalFunc, VmExternalFuncStatus};
use crate::Result;

const NAME: &str = "count";

// Reference: https://www.mongodb.com/docs/manual/reference/operator/aggregation/count/
// Distinct from count operator
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
        let arg0 = &args[0];
        if arg0.as_null().is_some() {  // complete
            self.is_completed.store(true, Ordering::Relaxed);
            let mut doc = Document::new();
            doc.insert(self.count_name.clone(), self.count.load(Ordering::Relaxed) as i64);
            return Ok(VmExternalFuncStatus::Next(doc.into()));
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(VmExternalFuncStatus::Continue)
    }

    fn is_completed(&self) -> bool {
        true
    }
}
