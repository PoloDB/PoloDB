/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use crate::{DbResult, LsmKv, TransactionType};
use crate::lsm::LsmSession;
use crate::lsm::multi_cursor::MultiCursor;

pub(crate) struct SessionInner {
    kv_session: LsmSession,
    auto_count: i32,
}

impl SessionInner {

    pub fn new(kv_session: LsmSession) -> SessionInner {
        SessionInner {
            kv_session,
            auto_count: 0,
        }
    }

    #[inline]
    pub fn kv_session(&self) -> &LsmSession {
        &self.kv_session
    }

    #[inline]
    pub fn kv_session_mut(&mut self) -> &mut LsmSession {
        &mut self.kv_session
    }

    #[inline]
    pub fn update_cursor_current(&mut self, cursor: &mut MultiCursor, value: &[u8]) -> DbResult<bool> {
        self.kv_session.update_cursor_current(cursor, value)
    }

    #[inline]
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> DbResult<()> {
        self.kv_session.put(key, value)
    }

    #[inline]
    pub fn delete_cursor_current(&mut self, cursor: &mut MultiCursor) -> DbResult<bool> {
        self.kv_session.delete_cursor_current(cursor)
    }

    pub fn auto_start_transaction(&mut self, ty: TransactionType) -> DbResult<()> {
        if self.auto_count == 0 {
            if self.kv_session.transaction().is_some() {  // manually
                return Ok(());
            }

            self.kv_session.start_transaction(ty)?;  // auto
        }

        self.auto_count += 1;

        Ok(())
    }

    pub fn auto_commit(&mut self, kv_engine: &LsmKv) -> DbResult<()> {
        if self.auto_count == 0 {
            return Ok(());
        }

        self.auto_count -= 1;

        if self.auto_count == 0 {
            kv_engine.inner.commit(&mut self.kv_session)?;
        }

        Ok(())
    }

}
