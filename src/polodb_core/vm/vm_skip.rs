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

use std::sync::atomic::{AtomicUsize, Ordering};
use crate::vm::vm_external_func::{VmExternalFunc, VmExternalFuncStatus};
use bson::Bson;
use crate::{Error, Result};
use crate::errors::mk_invalid_aggregate_field;

const NAME: &str = "skip";

pub(crate) struct VmFuncSkip {
    remain: AtomicUsize
}

impl VmFuncSkip {
    pub(crate) fn compile(paths: &mut Vec<String>, skip_val: &Bson) -> Result<Box<dyn VmExternalFunc>> {
        let skip = match skip_val {
            Bson::Int32(val) => *val as usize,
            Bson::Int64(val) => *val as usize,
            _ => {
                let invalid_err = mk_invalid_aggregate_field(paths);
                return Err(Error::InvalidField(invalid_err))
            }
        };
        Ok(Box::new(VmFuncSkip {
            remain: AtomicUsize::new(skip)
        }))
    }
}

impl VmExternalFunc for VmFuncSkip {
    fn name(&self) -> &str {
        NAME
    }

    fn call(&self, args: &[Bson]) -> Result<VmExternalFuncStatus> {
        let arg0 = &args[0];
        if arg0.as_null().is_some() {  // complete
            self.remain.store(0, Ordering::Relaxed);
            return Ok(VmExternalFuncStatus::Next(Bson::Null));
        }

        match self.remain.load(Ordering::Relaxed) {
            0 => Ok(VmExternalFuncStatus::Next(args[0].clone())),  // TODO(optimize): reduce clone
            _ => {
                self.remain.fetch_sub(1, Ordering::Relaxed);
                Ok(VmExternalFuncStatus::Continue)
            }
        }
    }

    fn is_completed(&self) -> bool {
        true
    }
}
