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

    pub fn minor_compact(&self) -> usize {
        self.inner.minor_compact()
    }

    pub fn add_major_compact(&self) {
        self.inner.add_major_compact()
    }

    pub fn major_compact(&self) -> usize {
        self.inner.major_compact()
    }

    pub fn set_free_segments_count(&self, count: usize) {
        self.inner.set_free_segments_count(count)
    }

    pub fn free_segments_count(&self) -> usize {
        self.inner.free_segments_count()
    }

    pub fn add_use_free_segment_count(&self) {
        self.inner.add_use_free_segment_count()
    }

    pub fn use_free_segment_count(&self) -> usize {
        self.inner.use_free_segment_count()
    }

    pub fn add_clone_snapshot_count(&self) {
        self.inner.clone_snapshot_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn clone_snapshot_count(&self) -> usize {
        self.inner.clone_snapshot_count.load(Ordering::SeqCst)
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
    major_compact: AtomicUsize,
    free_segments_count: AtomicUsize,
    use_free_segment_count: AtomicUsize,
    clone_snapshot_count: AtomicUsize,
}

impl LsmMetricsInner {

    #[inline]
    fn enable(&self) {
        self.enable.store(true, Ordering::Relaxed);
    }

    fn add_sync_count(&self) {
        test_enable!(self);
        self.sync_count.fetch_add(1, Ordering::Relaxed);
    }

    fn add_minor_compact(&self) {
        test_enable!(self);
        self.minor_compact.fetch_add(1, Ordering::Relaxed);
    }

    fn minor_compact(&self) -> usize {
        self.minor_compact.load(Ordering::Relaxed)
    }

    fn add_major_compact(&self) {
        test_enable!(self);
        self.major_compact.fetch_add(1, Ordering::Relaxed);
    }

    fn major_compact(&self) -> usize {
        self.major_compact.load(Ordering::Relaxed)
    }

    fn set_free_segments_count(&self, count: usize) {
        self.free_segments_count.store(count, Ordering::Relaxed);
    }

    fn free_segments_count(&self) -> usize {
        self.free_segments_count.load(Ordering::Relaxed)
    }

    fn add_use_free_segment_count(&self) {
        self.use_free_segment_count.fetch_add(1, Ordering::Relaxed);
    }

    fn use_free_segment_count(&self) -> usize {
        self.use_free_segment_count.load(Ordering::Relaxed)
    }

}

impl Default for LsmMetricsInner {

    fn default() -> Self {
        LsmMetricsInner {
            enable: AtomicBool::new(false),
            sync_count: AtomicUsize::new(0),
            minor_compact: AtomicUsize::new(0),
            major_compact: AtomicUsize::new(0),
            free_segments_count: AtomicUsize::new(0),
            use_free_segment_count: AtomicUsize::new(0),
            clone_snapshot_count: AtomicUsize::new(0),
        }
    }

}
