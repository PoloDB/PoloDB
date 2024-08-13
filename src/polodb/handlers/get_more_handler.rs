use std::sync::Arc;
use anyhow::{anyhow, Result};
use crate::handlers::{Handler, DEFAULT_BATCH_SIZE};
use async_trait::async_trait;
use bson::{rawdoc, Document, RawDocumentBuf};
use log::debug;
use polodb_core::ClientCursor;
use crate::app_context::AppContext;
use crate::reply::Reply;
use crate::wire;

pub(crate) struct GetMoreHandler {}

impl GetMoreHandler {

    pub(crate) fn new() -> Arc<dyn Handler> {
        Arc::new(GetMoreHandler {})
    }

    fn mk_cursor_doc(db_name: &str, col_name: &str, batch_size: isize, cursor_id: i64, cursor: &mut ClientCursor<Document>) -> Result<(RawDocumentBuf, bool)> {
        let mut next_batch_arr = bson::raw::RawArrayBuf::new();
        let mut count: isize = 0;
        let mut has_more = false;
        while cursor.advance()? {
            let doc = cursor.deserialize_current()?;
            let doc_bytes = bson::to_vec(&doc)?;
            next_batch_arr.push(bson::raw::RawBson::Document(RawDocumentBuf::from_bytes(doc_bytes)?));
            count += 1;
            if batch_size >= 0 && count >= batch_size {
                has_more = true;
                break;
            }
        }
        debug!("next_batch_arr count: {}", count);

        let doc = rawdoc! {
            "id": if has_more { cursor_id } else { 0 },
            "ns": format!("{}.{}", db_name, col_name),
            "nextBatch": next_batch_arr,
        };
        Ok((doc, has_more))
    }

}

#[async_trait]
impl Handler for GetMoreHandler {

    fn test(&self, doc: &RawDocumentBuf) -> anyhow::Result<bool> {
        let val = doc.get("getMore")?;
        Ok(val.is_some())
    }

    async fn handle(&self, ctx: AppContext, conn_id: u64, message: &wire::Message) -> anyhow::Result<Reply> {
        let doc = &message.document_payload;

        let db_name = match doc.get("$db")? {
            Some(val) => {
                val.as_str().ok_or(anyhow!("$db is not a string"))?
            },
            None => {
                return Err(anyhow!("$db is missing"));
            },
        };

        let collection = match doc.get("collection")? {
            Some(val) => {
                val.as_str().ok_or(anyhow!("collection is not a string"))?
            },
            None => {
                return Err(anyhow!("collection is missing"));
            },
        };

        let batch_size = match doc.get("batchSize")? {
            Some(val) => {
                val.as_i32().unwrap_or(DEFAULT_BATCH_SIZE)
            },
            None => DEFAULT_BATCH_SIZE,
        };

        let cursor_id = match doc.get("getMore")? {
            Some(v) => {
                v.as_i64().ok_or(anyhow::anyhow!("getMore field is not an Int64"))?
            }
            _ => return Err(anyhow::anyhow!("getMore field is not an Int64")),
        };

        let (cursor_doc, has_more) = {
            let cursor = ctx.get_cursor(cursor_id).ok_or(anyhow::anyhow!("cursor not found"))?;
            let mut cursor_guard = cursor.lock().unwrap();
            GetMoreHandler::mk_cursor_doc(db_name, collection, batch_size as isize, cursor_id, &mut cursor_guard)?
        };

        if !has_more {
            ctx.remove_cursor(&[cursor_id]);
            debug!("cursor removed: {}", cursor_id);
        }

        let body = rawdoc! {
            "ok": 1,
            "cursor": cursor_doc,
        };
        let reply = Reply::new(message.request_id.unwrap(), body);
        Ok(reply)
    }

}
