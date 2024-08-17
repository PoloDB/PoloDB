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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

#[allow(dead_code)]
impl Metrics {

    pub(crate) fn new() -> Metrics {
        let inner = Arc::new(MetricsInner::new());
        Metrics {
            inner,
        }
    }

    pub fn enable(&self) {
        self.inner.enable()
    }

    #[inline]
    pub(crate) fn add_find_by_index_count(&self) {
        self.inner.add_find_by_index_count();
    }

    pub fn find_by_index_count(&self) -> usize {
        self.inner.find_by_index_count.load(Ordering::SeqCst)
    }

}

struct MetricsInner {
    enable: AtomicBool,
    find_by_index_count: AtomicUsize,
}

macro_rules! test_enable {
    ($self:ident) => {
        if !$self.enable.load(Ordering::Relaxed) {
            return;
        }
    }
}

#[allow(dead_code)]
impl MetricsInner {

    fn new() -> MetricsInner {
        MetricsInner {
            enable: AtomicBool::new(false),
            find_by_index_count: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn enable(&self) {
        self.enable.store(true, Ordering::Relaxed);
    }

    pub(crate) fn add_find_by_index_count(&self) {
        test_enable!(self);

        self.find_by_index_count.fetch_add(1, Ordering::SeqCst);
    }

}

