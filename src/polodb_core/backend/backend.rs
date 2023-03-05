/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;
use bson::oid::ObjectId;
use crate::DbResult;
use crate::page::RawPage;
use crate::transaction::TransactionType;

#[derive(Debug, Copy, Clone)]
pub(crate) struct AutoStartResult {
    pub auto_start: bool,
}

pub(crate) trait Backend {
    fn read_page(&self, page_id: u32, session_id: Option<&ObjectId>) -> DbResult<Arc<RawPage>>;
    fn write_page(&mut self, page: &RawPage, session_id: Option<&ObjectId>) -> DbResult<()>;
    fn commit(&mut self) -> DbResult<()>;
    fn db_size(&self) -> u64;
    fn set_db_size(&mut self, size: u64) -> DbResult<()>;
    fn transaction_type(&self) -> Option<TransactionType>;
    fn upgrade_read_transaction_to_write(&mut self) -> DbResult<()>;
    fn rollback(&mut self) -> DbResult<()>;
    fn start_transaction(&mut self, ty: TransactionType) -> DbResult<()>;

    fn new_session(&mut self, id: &ObjectId) -> DbResult<()>;
    fn remove_session(&mut self, id: &ObjectId) -> DbResult<()>;
}
