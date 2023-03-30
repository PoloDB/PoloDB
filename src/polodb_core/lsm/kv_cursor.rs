/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::{Arc, Mutex, Weak};
use crate::{DbErr, DbResult};
use crate::lsm::lsm_kv::LsmKvInner;
use crate::lsm::multi_cursor::MultiCursor;

#[derive(Clone)]
pub struct KvCursor {
    db: Weak<LsmKvInner>,
    inner: Arc<Mutex<MultiCursor>>,
}

impl KvCursor {

    pub(crate) fn new(db: Arc<LsmKvInner>, multi_cursor: MultiCursor) -> KvCursor {
        let db = Arc::downgrade(&db);
        KvCursor {
            db,
            inner: Arc::new(Mutex::new(multi_cursor)),
        }
    }

    pub fn seek<K>(&self, key: K) -> DbResult<()>
    where
        K: AsRef<[u8]>
    {
        let mut cursor = self.inner.lock()?;
        cursor.seek(key.as_ref())
    }

    pub fn value(&self) -> DbResult<Option<Vec<u8>>> {
        let db = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        let cursor = self.inner.lock()?;
        cursor.value(db.as_ref())
    }

    pub fn key(&self) -> DbResult<Option<Arc<[u8]>>> {
        let cursor = self.inner.lock()?;
        Ok(cursor.key())
    }

    pub fn next(&self) -> DbResult<()> {
        let mut cursor = self.inner.lock()?;
        cursor.next()
    }

}
