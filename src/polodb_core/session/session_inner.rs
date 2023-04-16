/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use crate::{DbErr, DbResult, TransactionType};
use crate::lsm::LsmSession;
use crate::lsm::multi_cursor::MultiCursor;
use crate::transaction::TransactionState;

pub(crate) struct SessionInner {
    kv_session: LsmSession,
    transaction_state: TransactionState,
}

impl SessionInner {

    pub fn new(kv_session: LsmSession) -> SessionInner {
        SessionInner {
            kv_session,
            transaction_state: TransactionState::NoTrans,
        }
    }

    #[inline]
    pub fn kv_session(&self) -> &LsmSession {
        &self.kv_session
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
        match &self.transaction_state {
            TransactionState::DbAuto(counter) => {
                counter.set(counter.get() + 1)
            }
            TransactionState::NoTrans => {
                self.kv_session.start_transaction(ty)?;  // auto
                self.transaction_state = TransactionState::new_db_auto();
            }
            TransactionState::User => {
                if ty == TransactionType::Write {
                    self.kv_session.upgrade_to_write_if_needed()?;
                }
            }
        };
        Ok(())
    }

    pub fn auto_commit(&mut self) -> DbResult<()> {
        if let TransactionState::DbAuto(counter) = &self.transaction_state {
            if counter.get() == 0 {
                return Ok(());
            }
            counter.set(counter.get() - 1);
            if counter.get() == 0 {
                self.kv_session.commit_transaction()?;
            }
        }

        Ok(())
    }

    pub fn auto_rollback(&mut self) -> DbResult<()> {
        if let TransactionState::DbAuto(counter) = &self.transaction_state {
            if counter.get() == 0 {
                return Ok(());
            }
            counter.set(counter.get() - 1);
            if counter.get() == 0 {
                self.kv_session.abort_transaction()?;
            }
        }

        Ok(())
    }

    pub fn start_transaction(&mut self, ty: Option<TransactionType>) -> DbResult<()> {
        if self.transaction_state != TransactionState::NoTrans {
            return Err(DbErr::StartTransactionInAnotherTransaction);
        }
        self.kv_session.start_transaction(ty.unwrap_or(TransactionType::Read))?;
        self.transaction_state = TransactionState::User;
        Ok(())
    }

    pub fn commit_transaction(&mut self) -> DbResult<()> {
        if self.transaction_state != TransactionState::User {
            return Err(DbErr::NoTransactionStarted);
        }

        self.kv_session.commit_transaction()?;

        self.transaction_state = TransactionState::NoTrans;
        Ok(())
    }

    pub fn abort_transaction(&mut self) -> DbResult<()> {
        self.kv_session.abort_transaction()?;
        self.transaction_state = TransactionState::NoTrans;
        Ok(())
    }

}
