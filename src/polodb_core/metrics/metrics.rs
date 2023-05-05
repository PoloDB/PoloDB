/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
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

