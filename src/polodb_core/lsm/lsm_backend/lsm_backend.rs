/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;
use crate::DbResult;
use crate::lsm::lsm_segment::LsmTuplePtr;
use crate::lsm::lsm_snapshot::LsmSnapshot;
use crate::lsm::mem_table::MemTable;

pub(crate) trait LsmBackend: Send + Sync {
    fn read_segment_by_ptr(&self, ptr: LsmTuplePtr) -> DbResult<Arc<[u8]>>;
    fn read_latest_snapshot(&self) -> DbResult<LsmSnapshot>;
    fn sync_latest_segment(&self, segment: &MemTable, snapshot: &mut LsmSnapshot) -> DbResult<()>;
    fn minor_compact(&self, snapshot: &mut LsmSnapshot) -> DbResult<()>;
    fn major_compact(&self, snapshot: &mut LsmSnapshot) -> DbResult<()>;
    fn checkpoint_snapshot(&self, snapshot: &mut LsmSnapshot) -> DbResult<()>;
}
