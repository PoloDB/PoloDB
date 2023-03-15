/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::cmp::Ordering;
use std::sync::{Arc, Weak};
use crate::{DbErr, DbResult};
use crate::lsm::lsm_kv::LsmKvInner;

#[derive(Clone)]
pub struct KvCursor {
    meta_id: u64,  // verify if the cursor changed
    db: Weak<LsmKvInner>,
}

impl KvCursor {

    pub(crate) fn new(db: Arc<LsmKvInner>) -> KvCursor {
        let meta_id = db.meta_id();
        let db = Arc::downgrade(&db);
        KvCursor {
            meta_id,
            db,
        }
    }

    pub fn seek(&self, _key: &[u8], _ord: Ordering) -> DbResult<()> {
        let _db = self.db.upgrade().ok_or(DbErr::DbIsClosed)?;
        Ok(())
    }

}
