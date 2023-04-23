/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use crate::{Result, TransactionType};
use crate::session::SessionInner;

/// A PoloDB client session. This struct represents a logical session used for ordering sequential
/// operations. To create a `ClientSession`, call `start_session` on a `Database`.
pub struct ClientSession {
    pub(crate) inner: SessionInner,
}

impl ClientSession {

    pub(crate) fn new(inner: SessionInner) -> ClientSession {
        ClientSession {
           inner,
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
    pub fn start_transaction(&mut self, ty: Option<TransactionType>) -> Result<()> {
        self.inner.start_transaction(ty)
    }

    pub fn commit_transaction(&mut self) -> Result<()> {
        self.inner.commit_transaction()
    }

    pub fn abort_transaction(&mut self) -> Result<()> {
        self.inner.abort_transaction()
    }
}
