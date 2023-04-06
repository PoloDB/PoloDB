/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::{Arc, Mutex};
use web_sys::IdbDatabase;
use hashbrown::HashMap;
use crate::DbResult;
use crate::lsm::lsm_backend::LsmBackend;
use crate::lsm::lsm_segment::LsmTuplePtr;
use crate::lsm::lsm_snapshot::LsmSnapshot;
use crate::lsm::mem_table::MemTable;

pub(crate) struct IndexeddbBackend {
    db: Option<IdbDatabase>,
    snapshot: Mutex<Option<LsmSnapshot>>,
    data_value: HashMap<LsmTuplePtr, Arc<[u8]>>,
}

// in wasm, do NOT support multi-thread currently
unsafe impl Sync for IndexeddbBackend {}
unsafe impl Send for IndexeddbBackend {}

impl IndexeddbBackend {

    pub fn open(name: &str) -> DbResult<IndexeddbBackend> {
        Ok(IndexeddbBackend {
            db: None,
            snapshot: Mutex::new(None),
            data_value: HashMap::new(),
        })
    }

}

impl LsmBackend for IndexeddbBackend {
    fn read_segment_by_ptr(&self, ptr: LsmTuplePtr) -> DbResult<Arc<[u8]>> {
        let result = self.data_value.get(&ptr).unwrap().clone();
        Ok(result)
    }

    fn read_latest_snapshot(&self) -> DbResult<LsmSnapshot> {
        let snapshot = self.snapshot.lock()?;
        Ok(snapshot.as_ref().unwrap().clone())
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
        let mut snapshot = self.snapshot.lock()?;
        *snapshot = Some(new_snapshot.clone());
        Ok(())
    }
}
