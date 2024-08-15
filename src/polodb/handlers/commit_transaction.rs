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
        let conn_id = ctx.conn_id;
        let req_id = ctx.message.request_id.unwrap();
        let conn_ctx = ctx.app_context.get_conn_ctx(conn_id as i64).ok_or(anyhow!("connection not found"))?;
        conn_ctx.commit_transaction()?;

        let body = rawdoc! {
            "ok": 1,
        };
        let reply = Reply::new(req_id, body);
        Ok(reply)
    }

}
