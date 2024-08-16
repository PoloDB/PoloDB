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
        let req_id = ctx.message.request_id.unwrap();
        {
            let session = ctx.session.as_ref().ok_or(anyhow!("session not found"))?;
            let ctx = session.get_transaction().ok_or(anyhow!("transaction not found"))?;
            ctx.rollback()?;
            session.clear_transaction();
        }

        let body = rawdoc! {
            "ok": 1,
        };
        let reply = Reply::new(req_id, body);
        Ok(reply)
    }

}
