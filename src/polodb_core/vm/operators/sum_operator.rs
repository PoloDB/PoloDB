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

use std::sync::atomic::AtomicU64;
use bson::Bson;
use crate::vm::operators::VmOperator;

pub(crate) struct SumOperator {
    inner: AtomicU64,
}

impl SumOperator {

    pub(crate) fn compile(_v: &Bson) -> Box<dyn VmOperator> {
        Box::new(SumOperator {
            inner: AtomicU64::new(0),
        })
    }

}

impl VmOperator for SumOperator {
    fn initial_value(&self) -> Bson {
        Bson::Int64(self.inner.load(std::sync::atomic::Ordering::Relaxed) as i64)
    }

    fn next(&self, _input: &Bson) -> Bson {
        self.inner.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Bson::Int64(self.inner.load(std::sync::atomic::Ordering::Relaxed) as i64)
    }

    fn complete(&self) -> Bson {
        Bson::Int64(self.inner.load(std::sync::atomic::Ordering::Relaxed) as i64)
    }
}
