mod hello_handler;
mod insert_handler;
mod find_handler;
mod kill_cursors_handler;
mod get_more_handler;
mod update_handler;
mod delete_handler;
mod commit_transaction;
mod abort_transaction;
mod aggregate_handler;
mod ping_handler;

use std::sync::Arc;
use bson::RawDocumentBuf;
use anyhow::Result;
use crate::reply::Reply;
use crate::wire;
use async_trait::async_trait;

pub(crate) use hello_handler::HelloHandler;
pub(crate) use insert_handler::InsertHandler;
pub(crate) use find_handler::FindHandler;
pub(crate) use kill_cursors_handler::KillCursorsHandler;
pub(crate) use get_more_handler::GetMoreHandler;
pub(crate) use update_handler::UpdateHandler;
pub(crate) use delete_handler::DeleteHandler;
pub(crate) use commit_transaction::CommitTransactionHandler;
pub(crate) use abort_transaction::AbortTransactionHandler;
pub(crate) use aggregate_handler::AggregateHandle;
pub(crate) use ping_handler::PingHandler;
use crate::app_context::AppContext;
use crate::session_context::SessionContext;

pub(crate) const DEFAULT_BATCH_SIZE: i32 = 101;

pub(crate) struct HandleContext<'a> {
    pub(crate) app_context: AppContext,
    pub(crate) conn_id: u64,
    pub(crate) message: &'a wire::Message,
    pub(crate) session: Option<SessionContext>,
    pub(crate) auto_commit: bool,
}

#[async_trait]
pub(crate) trait Handler: Send + Sync {

    fn test(&self, doc: &RawDocumentBuf) -> Result<bool>;

    async fn handle(&self, ctx: &HandleContext) -> Result<Reply>;

}

pub(crate) fn make_handlers() -> Vec<Arc<dyn Handler>> {
    vec![
        FindHandler::new(),
        GetMoreHandler::new(),
        KillCursorsHandler::new(),
        AggregateHandle::new(),
        InsertHandler::new(),
        UpdateHandler::new(),
        DeleteHandler::new(),
        HelloHandler::new(),
        CommitTransactionHandler::new(),
        AbortTransactionHandler::new(),
        PingHandler::new(),
    ]
}
