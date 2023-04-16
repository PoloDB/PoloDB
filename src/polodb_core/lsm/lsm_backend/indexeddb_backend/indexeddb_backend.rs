/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::io::Write;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use bson::oid::ObjectId;
use hashbrown::HashMap;
use wasm_bindgen::prelude::*;
use js_sys::Reflect;
use crate::{DbErr, DbResult};
use crate::lsm::lsm_backend::indexeddb_backend::models::{IdbLog, IdbSegment};
use crate::lsm::lsm_backend::{format, lsm_log, LsmBackend, LsmLog};
use crate::lsm::lsm_backend::lsm_log::LsmCommitResult;
use crate::lsm::lsm_segment::{ImLsmSegment, LsmTuplePtr};
use crate::lsm::lsm_snapshot::LsmSnapshot;
use crate::lsm::lsm_tree::{LsmTree, LsmTreeValueMarker};
use crate::lsm::mem_table::MemTable;
use super::models::IdbMeta;
use byteorder::WriteBytesExt;
use crate::utils::vli;

#[wasm_bindgen(module = "/idb-adapter.js")]
extern "C" {
    async fn load_snapshot(db_name: &str) -> JsValue;

    type IdbBackendAdapter;

    #[wasm_bindgen(constructor)]
    fn new_backend(db: JsValue) -> IdbBackendAdapter;

    #[wasm_bindgen(method)]
    fn write_snapshot_to_idb(this: &IdbBackendAdapter, value: JsValue);

    #[wasm_bindgen(method)]
    fn write_segments_to_idb(this: &IdbBackendAdapter, value: JsValue);

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

    pub fn session_id(&self) -> ObjectId {
        let inner = self.inner.lock().unwrap();
        inner.session_id()
    }

    pub async fn load_snapshot(db_name: &str) -> JsValue {
        load_snapshot(db_name).await
    }

    pub fn open(init_data: JsValue) -> DbResult<IndexeddbBackend> {
        let oid_js = Reflect::get(&init_data, JsValue::from_str("session_id").as_ref()).unwrap();

        let session_id = if oid_js.is_string() {
            let str = oid_js.as_string().unwrap();
            ObjectId::from_str(&str).unwrap()
        } else {
            ObjectId::new()
        };

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

fn write_tuple_to_buffer(
    writer: &mut Vec<u8>,
    segments_id: &ObjectId,
    key: &[u8],
    value: LsmTreeValueMarker<&[u8]>
) -> DbResult<LsmTreeValueMarker<LsmTuplePtr>> {
    let offset = writer.len();
    match value {
        LsmTreeValueMarker::Value(insert_buffer) => {
            writer.write_u8(format::LSM_INSERT)?;
            vli::encode(writer, key.len() as i64)?;
            writer.write_all(key)?;

            let value_len = insert_buffer.len();
            vli::encode(writer, value_len as i64)?;
            writer.write_all(&insert_buffer)?;

            let end_offset = writer.len();

            let tuple = LsmTuplePtr::from_object_id(
                segments_id,
                offset as u32,
                (end_offset - offset) as u64,
            );
            Ok(LsmTreeValueMarker::Value(tuple))
        }
        LsmTreeValueMarker::Deleted => {
            writer.write_u8(format::LSM_POINT_DELETE)?;
            vli::encode(writer, key.len() as i64)?;
            writer.write_all(key)?;
            Ok(LsmTreeValueMarker::Deleted)
        }
        LsmTreeValueMarker::DeleteStart => {
            writer.write_u8(format::LSM_START_DELETE)?;
            vli::encode(writer, key.len() as i64)?;
            writer.write_all(key)?;
            Ok(LsmTreeValueMarker::DeleteStart)
        }
        LsmTreeValueMarker::DeleteEnd => {
            writer.write_u8(format::LSM_END_DELETE)?;
            vli::encode(writer, key.len() as i64)?;
            writer.write_all(key)?;
            Ok(LsmTreeValueMarker::DeleteEnd)
        }
    }
}

fn mem_table_to_segments(oid: ObjectId, mem_table: &MemTable) -> DbResult<(IdbSegment, LsmTree<Arc<[u8]>, LsmTuplePtr>)> {
    let mut result = vec![];

    let mut segments = LsmTree::<Arc<[u8]>, LsmTuplePtr>::new();

    let mut mem_table_cursor = mem_table.open_cursor();
    mem_table_cursor.go_to_min();

    while !mem_table_cursor.done() {
        let (key, value) = mem_table_cursor.tuple().unwrap();

        let pos = write_tuple_to_buffer(
            &mut result,
            &oid,
            key.as_ref(),
            value.as_ref(),
        )?;

        segments.update_in_place(key, pos);

        mem_table_cursor.next();
    }

    let s = IdbSegment::compress(oid, &result);
    Ok((s, segments))
}

impl LsmBackend for IndexeddbBackend {

    fn read_segment_by_ptr(&self, ptr: LsmTuplePtr) -> DbResult<Arc<[u8]>> {
        let inner = self.inner.lock()?;
        let result = inner.data_value.get(&ptr.pid).ok_or(DbErr::DbNotReady)?.clone();
        Ok(result)
    }

    fn read_latest_snapshot(&self) -> DbResult<LsmSnapshot> {
        let inner = self.inner.lock()?;
        Ok(inner.snapshot.clone())
    }

    fn sync_latest_segment(&self, segment: &MemTable, snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let inner = self.inner.lock()?;

        let segment_oid = ObjectId::new();
        let (segments_model, segments) = mem_table_to_segments(segment_oid, segment)?;

        let store_value = serde_wasm_bindgen::to_value(&segments_model).unwrap();

        inner.adapter.write_segments_to_idb(store_value);

        let im_seg = ImLsmSegment::from_object_id(segments, &segment_oid);
        snapshot.add_latest_segment(im_seg);

        Ok(())
    }

    fn minor_compact(&self, _snapshot: &mut LsmSnapshot) -> DbResult<()> {
        todo!()
    }

    fn major_compact(&self, _snapshot: &mut LsmSnapshot) -> DbResult<()> {
        todo!()
    }

    fn checkpoint_snapshot(&self, new_snapshot: &mut LsmSnapshot) -> DbResult<()> {
        let mut inner = self.inner.lock()?;
        inner.snapshot = new_snapshot.clone();

        let id_meta: IdbMeta = IdbMeta::from_snapshot(
            inner.session_id.clone(),
            &inner.snapshot,
        );
        let store_value = serde_wasm_bindgen::to_value(&id_meta).unwrap();
        inner.adapter.write_snapshot_to_idb(store_value);
        Ok(())
    }

}

struct IndexeddbBackendInner {
    session_id: ObjectId,
    adapter: IdbBackendAdapter,
    data_value: HashMap<u64, Arc<[u8]>>,
    snapshot: LsmSnapshot,
}

impl IndexeddbBackendInner {

    #[inline]
    fn session_id(&self) -> ObjectId {
        self.session_id.clone()
    }

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

            let meta = serde_wasm_bindgen::from_value::<IdbMeta>(meta_snapshot).unwrap();
            let snapshot = meta.generate_snapshot();

            let data_value = IndexeddbBackendInner::data_value_from_segments(segments);

            Ok(IndexeddbBackendInner {
                session_id,
                adapter,
                data_value,
                snapshot,
            })
        } else {
            let result = IndexeddbBackendInner {
                session_id,
                adapter,
                data_value: HashMap::new(),
                snapshot: LsmSnapshot::new(),
            };

            result.force_write_first_snapshot();

            Ok(result)
        }
    }

    fn force_write_first_snapshot(&self) {
        let id_meta = IdbMeta::from_snapshot(
            self.session_id.clone(),
            &self.snapshot,
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
    init_logs: JsValue,
    safe_clear: AtomicBool,
}

unsafe impl Sync for IndexeddbLog {}

unsafe impl Send for IndexeddbLog {}

impl IndexeddbLog {

    #[allow(dead_code)]
    pub fn new(session_id: ObjectId, init_data: JsValue) -> IndexeddbLog {
        let db = Reflect::get(&init_data, JsValue::from_str("db").as_ref()).unwrap();
        let init_logs = Reflect::get(&init_data, JsValue::from_str("logs_data").as_ref()).unwrap();
        let adapter = IdbLogAdapter::new_log(db);
        IndexeddbLog {
            session_id,
            adapter,
            init_logs,
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
            session: self.session_id.clone(),
        };

        let val = serde_wasm_bindgen::to_value(&commit_log).unwrap();
        self.adapter.commit(val);

        Ok(LsmCommitResult {
            offset: 0,
        })
    }

    fn update_mem_table_with_latest_log(&self, _snapshot: &LsmSnapshot, mem_table: &mut MemTable) -> DbResult<()> {
        use js_sys::Array;

        if self.init_logs.is_array() {
            let js_array = self.init_logs.clone().dyn_into::<Array>().unwrap();
            for i in 0..js_array.length() {
                let item = js_array.get(i);

                let idb_log_item = serde_wasm_bindgen::from_value::<IdbLog>(item).unwrap();

                lsm_log::lsm_log_utils::update_mem_table_by_buffer(
                    &idb_log_item.content,
                    0,
                    mem_table,
                    true,
                );
            }
        }

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
