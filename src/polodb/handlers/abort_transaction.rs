use std::sync::Arc;
use bson::{rawdoc, RawDocumentBuf};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use crate::handlers::{HandleContext, Handler};
use crate::reply::Reply;

pub(crate) struct AbortTransactionHandler;

impl AbortTransactionHandler {

    pub(crate) fn new() -> Arc<AbortTransactionHandler> {
        Arc::new(AbortTransactionHandler)
    }

}

#[async_trait]
impl Handler for AbortTransactionHandler {

    fn test(&self, doc: &RawDocumentBuf) -> anyhow::Result<bool> {
        let val = doc.get("abortTransaction")?;
        match val {
            Some(r) => Ok(r.as_i64().is_some()),
            None => Ok(false),
        }
    }

    async fn handle(&self, ctx: &HandleContext) -> Result<Reply> {
        let conn_id = ctx.conn_id;
        let req_id = ctx.message.request_id.unwrap();
        let conn_ctx = ctx.app_context.get_conn_ctx(conn_id as i64).ok_or(anyhow!("connection not found"))?;
        conn_ctx.abort_transaction()?;

        let body = rawdoc! {
            "ok": 1,
        };
        let reply = Reply::new(req_id, body);
        Ok(reply)
    }

}
