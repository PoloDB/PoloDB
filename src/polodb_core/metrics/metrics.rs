use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use bson::oid::ObjectId;
use hashbrown::HashMap;

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
    sid:   Option<ObjectId>,
}

impl Metrics {

    pub(crate) fn new() -> Metrics {
        let inner = Arc::new(MetricsInner::new());
        Metrics {
            inner,
            sid: None,
        }
    }

    pub fn enable(&self) {
        self.inner.enable()
    }

    pub fn data(&self) -> MetricsData {
        let inner = self.inner.data.lock().unwrap();
        inner.data.clone()
    }

    /// trace the data page allocation
    #[inline]
    pub(crate) fn add_data_page(&self, remain_size: u32) {
        self.inner.add_data_page(self.sid.as_ref(), remain_size)
    }

    #[inline]
    pub(crate) fn use_space_in_data_page(&self, used_size: u32) {
        self.inner.use_space_in_data_page(self.sid.as_ref(), used_size)
    }

    pub(crate) fn commit(&self) {
        self.inner.commit(self.sid.as_ref());
    }

    pub(crate) fn drop_session(&self) {
        self.inner.drop_session(self.sid.as_ref());
    }

    pub fn clone_with_sid(&self, sid: ObjectId) -> Metrics {
        Metrics {
            inner: self.inner.clone(),
            sid: Some(sid),
        }
    }

}

struct MetricsInner {
    enable: AtomicBool,
    data: Mutex<MetricsDataWrapper>,
}

macro_rules! test_enable {
    ($self:ident) => {
        if !$self.enable.load(Ordering::Relaxed) {
            return;
        }
    }
}

impl MetricsInner {

    fn new() -> MetricsInner {
        MetricsInner {
            enable: AtomicBool::new(false),
            data: Mutex::new(MetricsDataWrapper::new()),
        }
    }

    #[inline]
    fn enable(&self) {
        self.enable.store(true, Ordering::Relaxed);
    }

    pub(crate) fn add_data_page(&self, sid: Option<&ObjectId>, remain_size: u32) {
        test_enable!(self);

        let mut data_wrapper = self.data.lock().unwrap();

        let data = match sid {
            Some(sid) => data_wrapper.session.get_mut(sid).unwrap(),
            None => &mut data_wrapper.data,
        };
        data.data_page_count += 1;
        data.data_page_spaces += remain_size as usize;
    }

    pub(crate) fn use_space_in_data_page(&self, sid: Option<&ObjectId>,used_size: u32) {
        test_enable!(self);

        let mut data_wrapper = self.data.lock().unwrap();

        let data = match sid {
            Some(sid) => data_wrapper.session.get_mut(sid).unwrap(),
            None => &mut data_wrapper.data,
        };
        data.data_page_used_bytes += used_size as usize;
    }

    pub(crate) fn commit(&self, sid: Option<&ObjectId>) {
        test_enable!(self);

        if let Some(sid) = sid {
            let mut data_wrapper = self.data.lock().unwrap();
            let data = data_wrapper.session.get(sid).unwrap().clone();
            data_wrapper.data = data;
        }
    }

    pub(crate) fn drop_session(&self, sid: Option<&ObjectId>) {
        test_enable!(self);

        if let Some(sid) = sid {
            let mut data_wrapper = self.data.lock().unwrap();
            data_wrapper.session.remove(sid);
        }
    }

}

#[derive(Clone)]
pub struct MetricsData {
    pub data_page_count: usize,
    pub data_page_spaces: usize,
    pub data_page_used_bytes: usize,
}

impl MetricsData {

    pub fn data_used_ratio(&self) -> f64 {
        (self.data_page_used_bytes as f64) / (self.data_page_spaces as f64)
    }

}

impl Default for MetricsData {
    fn default() -> Self {
        MetricsData {
            data_page_count: 0,
            data_page_used_bytes: 0,
            data_page_spaces: 0,
        }
    }
}

struct MetricsDataWrapper {
    data: MetricsData,
    session: HashMap<ObjectId, MetricsData>,
}

impl MetricsDataWrapper {

    fn new() -> MetricsDataWrapper {
        MetricsDataWrapper {
            data: MetricsData::default(),
            session: HashMap::new(),
        }
    }

}
