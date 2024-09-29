use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use anyhow::{anyhow, Result};
use bson::{rawdoc, Document, RawDocumentBuf};
use polodb_core::CollectionT;
use crate::handlers::{FindHandler, HandleContext, Handler};
use crate::reply::Reply;

pub(crate) struct AggregateHandle;

impl AggregateHandle {

    pub fn new() -> Arc<dyn Handler> {
        Arc::new(AggregateHandle)
    }

}

#[async_trait]
impl Handler for AggregateHandle {
    fn test(&self, doc: &RawDocumentBuf) -> Result<bool> {
        let val = doc.get("aggregate")?;
        match val {
            Some(r) => Ok(r.as_str().is_some()),
            None => Ok(false),
        }
    }

    async fn handle(&self, ctx: &HandleContext) -> Result<Reply> {
        let req_id = ctx.message.request_id.unwrap();
        let col_name = ctx.message.document_payload.get_str("aggregate")?;

        let batch_size = -1;

        let doc = &ctx.message.document_payload;
        let db_name = match doc.get("$db")? {
            Some(val) => {
                val.as_str().ok_or(anyhow!("$db is not a string"))?
            },
            None => {
                return Err(anyhow!("$db is missing"));
            },
        };

        let pipeline = ctx.message.document_payload.get_array("pipeline")?;
        let mut pipeline_arr: Vec<Document> = vec![];

        for (index, doc) in pipeline.into_iter().enumerate() {
            let raw_bson = doc?;
            let doc = raw_bson.as_document().ok_or(anyhow!("the {}th element of pipeline should be a doc", index))?;
            let d = bson::from_slice::<Document>(doc.as_bytes())?;
            pipeline_arr.push(d);
        }

        let session_opt = ctx.session.clone();
        let cursor = if let Some(session) = session_opt {
            let txn = session.get_transaction().ok_or(anyhow!("transaction not started"))?;
            let collection = txn.collection::<Document>(col_name);
            collection.aggregate(pipeline_arr).run()?
        } else {
            let db = ctx.app_context.db();
            let collection = db.collection::<Document>(col_name);
            collection.aggregate(pipeline_arr).run()?
        };

        let cursor = Arc::new(Mutex::new(cursor));
        let cursor_id = ctx.app_context.save_cursor(cursor.clone());
        let cursor_doc = {
            let mut cursor_guard = cursor.lock().unwrap();
            FindHandler::mk_cursor_doc(cursor_id, db_name, col_name, &mut cursor_guard, batch_size as isize)?
        };

        let body = rawdoc! {
            "ok": 1,
            "cursor": cursor_doc,
        };
        let reply = Reply::new(req_id, body);
        Ok(reply)
    }

}
