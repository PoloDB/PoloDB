mod hello_handler;
mod insert_handler;
mod find_handler;
mod kill_cursors_handler;
mod get_more_handler;
mod update_handler;
mod delete_handler;

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
use crate::app_context::AppContext;

pub(crate) const DEFAULT_BATCH_SIZE: i32 = 101;

#[async_trait]
pub(crate) trait Handler: Send + Sync {

    fn test(&self, doc: &RawDocumentBuf) -> Result<bool>;

    async fn handle(&self, ctx: AppContext, conn_id: u64, message: &wire::Message) -> Result<Reply>;

}

