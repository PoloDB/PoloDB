/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::DbResult;
use crate::lsm::lsm_snapshot::LsmSnapshot;
use crate::lsm::mem_table::MemTable;

#[allow(dead_code)]
pub(crate) mod format {
    pub const EOF: u8     = 0x00;
    pub const PAD1: u8    = 0x01;
    pub const PAD2: u8    = 0x02;
    pub const COMMIT: u8  = 0x03;
    pub const JUMP: u8    = 0x04;
    pub const WRITE: u8   = 0x06;
    pub const DELETE: u8  = 0x08;
}

#[allow(dead_code)]
pub(crate) struct LsmCommitResult {
    pub offset: u64,
}

pub(crate) trait LsmLog: Send + Sync {

    fn start_transaction(&self) -> DbResult<()>;

    fn commit(&self, buffer: Option<&[u8]>) -> DbResult<LsmCommitResult>;

    fn update_mem_table_with_latest_log(
        &self,
        snapshot: &LsmSnapshot,
        mem_table: &mut MemTable,
    ) -> DbResult<()>;

    fn shrink(&self, snapshot: &mut LsmSnapshot) -> DbResult<()>;

    /// Sometimes we need to clear the log
    /// when the database is closing.
    ///
    /// But the log trait don't know if the database
    /// has sync all the data.
    /// If the data is not fully synced, it's not safe
    /// to clean the log.
    /// Otherwise, the log can be erased safely.
    fn enable_safe_clear(&self);

}
