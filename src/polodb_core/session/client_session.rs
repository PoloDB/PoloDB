/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use bson::oid::ObjectId;
use crate::{Database, DbResult, TransactionType};

/// A PoloDB client session. This struct represents a logical session used for ordering sequential
/// operations. To create a `ClientSession`, call `start_session` on a `Database`.
pub struct ClientSession<'a> {
    db: &'a Database,
    pub(crate) id: ObjectId,
}

impl<'a> ClientSession<'a> {

    pub(crate) fn new(db: &'a Database, id: ObjectId) -> ClientSession {
        ClientSession {
            db,
            id,
        }
    }

    /// Manually start a transaction. There are three types of transaction.
    ///
    /// - `None`: Auto transaction
    /// - `Some(Transaction::Write)`: Write transaction
    /// - `Some(Transaction::Read)`: Read transaction
    ///
    /// When you pass `None` to type parameter. The PoloDB will go into
    /// auto mode. The PoloDB will go into read mode firstly, once the users
    /// execute write operations(insert/update/delete), the DB will turn into
    /// write mode.
    pub fn start_transaction(&mut self, ty: Option<TransactionType>) -> DbResult<()> {
        self.db.start_transaction(ty, Some(&self.id))
    }

    pub fn commit_transaction(&mut self) -> DbResult<()> {
        self.db.commit(Some(&self.id))
    }

    pub fn abort_transaction(&mut self) -> DbResult<()> {
        self.db.rollback(Some(&self.id))
    }
}

impl Drop for ClientSession<'_> {
    fn drop(&mut self) {
        let drop_error = self.db.drop_session(&self.id);
        if let Err(err) = drop_error {
            crate::polo_log!("drop session error: {}", err);
        }
    }
}
