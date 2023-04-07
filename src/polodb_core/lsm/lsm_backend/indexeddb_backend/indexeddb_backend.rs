/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use bson::oid::ObjectId;
use hashbrown::HashMap;
use wasm_bindgen::prelude::*;
use js_sys::{Array, Reflect};
use crate::{DbErr, DbResult};
use crate::lsm::lsm_backend::indexeddb_backend::models::{IdbLog, IdbSegment};
use crate::lsm::lsm_backend::{LsmBackend, LsmLog};
use crate::lsm::lsm_backend::lsm_log::LsmCommitResult;
use crate::lsm::lsm_segment::LsmTuplePtr;
use crate::lsm::lsm_snapshot::LsmSnapshot;
use crate::lsm::mem_table::MemTable;
use super::models::IdbMeta;

#[wasm_bindgen(module = "/idb-adapter.js")]
extern "C" {
    async fn load_snapshot(db_name: &str) -> JsValue;

    type IdbBackendAdapter;

    #[wasm_bindgen(constructor)]
    fn new_backend(db: JsValue) -> IdbBackendAdapter;

    #[wasm_bindgen(method)]
    fn write_snapshot_to_idb(this: &IdbBackendAdapter, value: JsValue);

    #[wasm_bindgen(method)]
    fn dispose(this: &IdbBackendAdapter);

    type IdbLogAdapter;

    #[wasm_bindgen(constructor)]
    fn new_log(db: JsValue) -> IdbLogAdapter;

    #[wasm_bindgen(method)]
    fn commit(this: &IdbLogAdapter, content: JsValue);

    #[wasm_bindgen(method)]
    fn shrink(this: &IdbLogAdapter, session: JsValue);
}

pub struct IndexeddbBackend {
    inner: Arc<Mutex<IndexeddbBackendInner>>,
}

// in wasm, do NOT support multi-thread currently
unsafe impl Sync for IndexeddbBackend {}

unsafe impl Send for IndexeddbBackend {}

impl IndexeddbBackend {
    pub async fn load_snapshot(db_name: &str) -> JsValue {
        load_snapshot(db_name).await
    }

    pub fn open(session_id: ObjectId, init_data: JsValue) -> DbResult<IndexeddbBackend> {
        let inner = IndexeddbBackendInner::new(
            session_id,
            init_data,
        )?;
        let result = IndexeddbBackend {
            inner: Arc::new(Mutex::new(inner)),
        };

        Ok(result)
    }

}

fn js_array(values: &[&str]) -> JsValue {
    return JsValue::from(values.into_iter()
        .map(|x| JsValue::from_str(x))
        .collect::<Array>());
}

impl LsmBackend for IndexeddbBackend {

    fn read_segment_by_ptr(&self, ptr: LsmTuplePtr) -> DbResult<Arc<[u8]>> {
        let inner = self.inner.lock()?;
        let result = inner.data_value.get(&ptr.pid).ok_or(DbErr::DbNotReady)?.clone();
        Ok(result)
    }

    fn read_latest_snapshot(&self) -> DbResult<LsmSnapshot> {
        let inner = self.inner.lock()?;
        Ok(inner.snapshot.as_ref().unwrap().clone())
    }

    fn sync_latest_segment(&self, segment: &MemTable, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        todo!()
    }

    fn minor_compact(&self, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        todo!()
    }

    fn major_compact(&self, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        todo!()
    }

    fn checkpoint_snapshot(&self, new_snapshot: &mut LsmSnapshot) -> DbResult<()> {
        {
            let mut inner = self.inner.lock()?;
            inner.snapshot = Some(new_snapshot.clone());

            let id_meta: IdbMeta = IdbMeta::from_snapshot(
                inner.session_id.clone(),
                inner.snapshot.as_ref().unwrap(),
            );
            let store_value = serde_wasm_bindgen::to_value(&id_meta).unwrap();
            inner.adapter.write_snapshot_to_idb(store_value);
        }
        Ok(())
    }

}

struct IndexeddbBackendInner {
    session_id: ObjectId,
    adapter: IdbBackendAdapter,
    data_value: HashMap<u64, Arc<[u8]>>,
    snapshot: Option<LsmSnapshot>,
}

impl IndexeddbBackendInner {

    fn data_value_from_segments(segments: JsValue) -> HashMap<u64, Arc<[u8]>> {
        let mut result = HashMap::new();
        let segments_map = segments.dyn_into::<js_sys::Map>().unwrap();

        segments_map.for_each(&mut |key, value| {
            let rkey = key.as_f64().unwrap() as u64;

            let segment_data = serde_wasm_bindgen::from_value::<IdbSegment>(value).unwrap();

            result.insert(rkey, segment_data.decompress().unwrap().into());
        });

        result
    }

    fn new(session_id: ObjectId, init_data: JsValue) -> DbResult<IndexeddbBackendInner> {
        let db = Reflect::get(&init_data, JsValue::from_str("db").as_ref()).unwrap();
        let meta_snapshot = Reflect::get(&init_data, JsValue::from_str("snapshot").as_ref()).unwrap();

        let adapter = IdbBackendAdapter::new_backend(db);

        if meta_snapshot.is_object() {
            let segments = Reflect::get(&init_data, JsValue::from_str("segments").as_ref()).unwrap();

            let _meta = serde_wasm_bindgen::from_value::<IdbMeta>(meta_snapshot).unwrap();

            let data_value = IndexeddbBackendInner::data_value_from_segments(segments);

            Ok(IndexeddbBackendInner {
                session_id,
                adapter,
                data_value,
                snapshot: None,
            })
        } else {
            let result = IndexeddbBackendInner {
                session_id,
                adapter,
                data_value: HashMap::new(),
                snapshot: Some(LsmSnapshot::new()),
            };

            result.force_write_first_snapshot();

            Ok(result)
        }
    }

    fn force_write_first_snapshot(&self) {
        let id_meta = IdbMeta::from_snapshot(
            self.session_id.clone(),
            self.snapshot.as_ref().unwrap(),
        );

        let meta_js_value = serde_wasm_bindgen::to_value(&id_meta).unwrap();

        self.adapter.write_snapshot_to_idb(meta_js_value);
    }

}

impl Drop for IndexeddbBackendInner {

    fn drop(&mut self) {
        self.adapter.dispose();
    }

}

pub struct IndexeddbLog {
    session_id: ObjectId,
    adapter: IdbLogAdapter,
    safe_clear: AtomicBool,
}

unsafe impl Sync for IndexeddbLog {}

unsafe impl Send for IndexeddbLog {}

impl IndexeddbLog {

    pub fn new(session_id: ObjectId, init_data: JsValue) -> IndexeddbLog {
        let db = Reflect::get(&init_data, JsValue::from_str("db").as_ref()).unwrap();
        let adapter = IdbLogAdapter::new_log(db);
        IndexeddbLog {
            session_id,
            adapter,
            safe_clear: AtomicBool::new(false),
        }
    }

}

impl LsmLog for IndexeddbLog {
    fn start_transaction(&self) -> DbResult<()> {
        Ok(())
    }

    fn commit(&self, buffer: Option<&[u8]>) -> DbResult<LsmCommitResult> {
        if buffer.is_none() {
            return Ok(LsmCommitResult {
                offset: 0,
            });
        }

        let commit_log = IdbLog {
            content: buffer.unwrap().into(),
            session: Default::default(),
        };

        let val = serde_wasm_bindgen::to_value(&commit_log).unwrap();
        self.adapter.commit(val);

        Ok(LsmCommitResult {
            offset: 0,
        })
    }

    fn update_mem_table_with_latest_log(&self, snapshot: &LsmSnapshot, mem_table: &mut MemTable) -> DbResult<()> {
        Ok(())
    }

    fn shrink(&self, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let hex_id = self.session_id.to_hex();
        let val = serde_wasm_bindgen::to_value(&hex_id).unwrap();
        self.adapter.shrink(val);

        snapshot.log_offset = 0;

        Ok(())
    }

    fn enable_safe_clear(&self) {
        self.safe_clear.store(true, Ordering::Relaxed);
    }
}
