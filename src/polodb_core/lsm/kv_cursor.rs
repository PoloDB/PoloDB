/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::{Arc, Mutex, Weak};
use crate::DbResult;
use crate::lsm::lsm_kv::LsmKvInner;
use super::multi_cursor::MultiCursor;

#[derive(Clone)]
pub struct KvCursor {
    meta_id: u64,  // verify if the cursor changed
    db: Weak<LsmKvInner>,
    inner: Arc<Mutex<MultiCursor>>,
}

impl KvCursor {

    pub(crate) fn new(db: Arc<LsmKvInner>, multi_cursor: MultiCursor) -> KvCursor {
        let meta_id = db.meta_id();
        let db = Arc::downgrade(&db);
        KvCursor {
            meta_id,
            db,
            inner: Arc::new(Mutex::new(multi_cursor)),
        }
    }

    pub fn seek(&self, key: &[u8]) -> DbResult<()> {
        let mut cursor = self.inner.lock()?;
        cursor.seek(key)
    }

}
