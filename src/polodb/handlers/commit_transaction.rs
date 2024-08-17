use std::sync::Arc;
use async_trait::async_trait;
use bson::{rawdoc, RawDocumentBuf};
use crate::handlers::{HandleContext, Handler};
use crate::reply::Reply;
use anyhow::{Result, anyhow};

pub(crate) struct CommitTransactionHandler;

impl CommitTransactionHandler {

    pub(crate) fn new() -> Arc<CommitTransactionHandler> {
        Arc::new(CommitTransactionHandler)
    }

}

#[async_trait]
impl Handler for CommitTransactionHandler {

    fn test(&self, doc: &RawDocumentBuf) -> anyhow::Result<bool> {
        let val = doc.get("commitTransaction")?;
        match val {
            Some(r) => {
                match r {
                    bson::RawBsonRef::Int32(i) => Ok(i == 1),
                    bson::RawBsonRef::Int64(i) => Ok(i == 1),
                    _ => Ok(false),
                }
            },
            None => Ok(false),
        }
    }

    async fn handle(&self, ctx: &HandleContext) -> Result<Reply> {
        let req_id = ctx.message.request_id.unwrap();
        let session = ctx.session.as_ref().ok_or(anyhow!("session not found"))?;
        let conn_ctx = session.get_transaction().ok_or(anyhow!("transaction not found"))?;
        conn_ctx.commit()?;

        let body = rawdoc! {
            "ok": 1,
        };
        let reply = Reply::new(req_id, body);
        Ok(reply)
    }

}
