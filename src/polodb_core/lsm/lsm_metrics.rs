use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Clone)]
pub struct LsmMetrics {
    inner: Arc<LsmMetricsInner>,
}

impl LsmMetrics {

    pub fn new() -> LsmMetrics {
        let inner = LsmMetricsInner::default();
        LsmMetrics {
            inner: Arc::new(inner),
        }
    }

    pub fn enable(&self) {
        self.inner.enable()
    }

    pub fn add_sync_count(&self) {
        self.inner.add_sync_count()
    }

    pub fn sync_count(&self) -> usize {
        self.inner.sync_count.load(Ordering::Relaxed)
    }

    pub fn add_minor_compact(&self) {
        self.inner.add_minor_compact();
    }

}

macro_rules! test_enable {
    ($self:ident) => {
        if !$self.enable.load(Ordering::Relaxed) {
            return;
        }
    }
}

struct LsmMetricsInner {
    enable: AtomicBool,
    sync_count: AtomicUsize,
    minor_compact: AtomicUsize,
}

impl LsmMetricsInner {

    #[inline]
    fn enable(&self) {
        self.enable.store(true, Ordering::Relaxed);
    }

    pub fn add_sync_count(&self) {
        test_enable!(self);
        self.sync_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_minor_compact(&self) {
        test_enable!(self);
        self.minor_compact.fetch_add(1, Ordering::Relaxed);
    }

}

impl Default for LsmMetricsInner {

    fn default() -> Self {
        LsmMetricsInner {
            enable: AtomicBool::new(false),
            sync_count: AtomicUsize::new(0),
            minor_compact: AtomicUsize::new(0),
        }
    }

}
