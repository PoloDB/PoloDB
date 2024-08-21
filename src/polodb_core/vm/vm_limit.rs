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
use bson::Bson;
use crate::vm::vm_external_func::{VmExternalFunc, VmExternalFuncStatus};
use crate::{Error, Result};
use crate::errors::mk_invalid_aggregate_field;

pub(crate) struct VmFuncLimit {
    remain: AtomicUsize,
}

impl VmFuncLimit {

    pub(crate) fn compile(paths: &mut Vec<String>, bson: &Bson) -> Result<Box<dyn VmExternalFunc>> {
        let limit = match bson {
            Bson::Int32(val) => *val as usize,
            Bson::Int64(val) => *val as usize,
            _ => {
                let invalid_err = mk_invalid_aggregate_field(paths);
                return Err(Error::InvalidField(invalid_err));
            }
        };
        Ok(Box::new(VmFuncLimit {
            remain: AtomicUsize::new(limit)
        }))
    }

}

impl VmExternalFunc for VmFuncLimit {
    fn name(&self) -> &str {
        "limit"
    }

    fn call(&self, args: &[Bson]) -> Result<VmExternalFuncStatus> {
        let arg0 = &args[0];
        if arg0.as_null().is_some() {  // complete
            self.remain.store(0, Ordering::Relaxed);
            return Ok(VmExternalFuncStatus::Next(Bson::Null));
        }

        match self.remain.load(Ordering::Relaxed) {
            0 => Ok(VmExternalFuncStatus::Next(Bson::Null)),
            _ => {
                self.remain.fetch_sub(1, Ordering::Relaxed);
                Ok(VmExternalFuncStatus::Next(args[0].clone()))
            }
        }
    }

    fn is_completed(&self) -> bool {
        true
    }
}
