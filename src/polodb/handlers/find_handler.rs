// Copyright 2024 Vincent Chan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::{Arc, Mutex};
use anyhow::{anyhow, Result};
use bson::{rawdoc, Document, RawArrayBuf, RawBson, RawDocumentBuf};
use crate::handlers::{HandleContext, Handler, DEFAULT_BATCH_SIZE};
use crate::reply::Reply;
use async_trait::async_trait;
use log::debug;
use polodb_core::{ClientCursor, CollectionT};

pub(crate) struct FindHandler {}

impl FindHandler {

    pub fn new() -> Arc<dyn Handler> {
        Arc::new(FindHandler {})
    }

    fn handle_single_batch(ctx: &HandleContext, db_name: &str, col_name: &str, cursor: &mut ClientCursor<Document>) -> Result<Reply> {
        let cursor_doc = FindHandler::mk_cursor_doc(0, db_name, col_name, cursor, -1)?;
        let body = rawdoc! {
            "ok": 1,
            "cursor": cursor_doc,
        };
        let req_id = ctx.message.request_id.unwrap();
        let reply = Reply::new(req_id, body);
        Ok(reply)
    }

    fn mk_cursor_doc(cursor_id: i64, db_name: &str, col_name: &str, cursor: &mut ClientCursor<Document>, batch_size: isize) -> Result<RawDocumentBuf> {
        let mut doc = rawdoc! {};

        let (first_batch, _has_more) = FindHandler::consume_first_batch(cursor, batch_size)?;
        doc.append("firstBatch", RawBson::Array(first_batch));
        if cursor_id >= 0 {
            doc.append("id", RawBson::from(cursor_id));
        }

        let ns = format!("{}.{}", db_name, col_name);
        doc.append("ns", RawBson::from(ns));

        Ok(doc)
    }

    fn consume_first_batch(cursor: &mut ClientCursor<Document>, batch_size: isize) -> Result<(RawArrayBuf, bool)> {
        let mut raw_arr = RawArrayBuf::new();
        let mut has_more = false;
        let mut count: isize = 0;

        while cursor.advance()? {
            let doc = cursor.deserialize_current()?;
            let doc_bytes = bson::to_vec(&doc)?;
            raw_arr.push(RawBson::Document(RawDocumentBuf::from_bytes(doc_bytes)?));
            count += 1;
            if batch_size >= 0 && count >= batch_size {
                has_more = true;
                break;
            }
        }

        Ok((raw_arr, has_more))
    }

}

#[async_trait]
impl Handler for FindHandler {
    fn test(&self, doc: &RawDocumentBuf) -> anyhow::Result<bool> {
        let val = doc.get("find")?;
        Ok(val.is_some())
    }

    async fn handle(&self, ctx: &HandleContext) -> Result<Reply> {
        let doc = &ctx.message.document_payload;
        let collection_name = doc.get("find")?.unwrap().as_str().ok_or(anyhow!("find field is not a string"))?;
        let db = ctx.app_context.db();

        let db_name = match doc.get("$db")? {
            Some(val) => {
                val.as_str().ok_or(anyhow!("$db is not a string"))?
            },
            None => {
                return Err(anyhow!("$db is missing"));
            },
        };

        let single_batch = match doc.get("singleBatch")? {
            Some(val) => {
                val.as_bool().unwrap_or(false)
            },
            None => false,
        };

        let filter = match doc.get("filter")? {
            Some(val) => {
                let doc = val.as_document().ok_or(anyhow!("filter is not a document"))?;
                bson::from_slice::<bson::Document>(doc.as_bytes())?
            },
            None => {
                Document::new()
            },
        };

        let batch_size = match doc.get("batchSize")? {
            Some(val) => {
                val.as_i32().unwrap_or(DEFAULT_BATCH_SIZE)
            },
            None => DEFAULT_BATCH_SIZE,
        };

        let session_opt = ctx.session.clone();
        debug!("find collection: {}, auto commit: {}", collection_name, ctx.auto_commit);
        let mut cursor = if let Some(session) = session_opt {
            let txn = session.get_transaction().ok_or(anyhow!("transaction not started"))?;
            let collection = txn.collection::<Document>(collection_name);
            collection.find(Some(filter))?
        } else {
            let collection = db.collection::<Document>(collection_name);
            collection.find(Some(filter))?
        };
        if single_batch {
            return FindHandler::handle_single_batch(ctx, &db_name, &collection_name, &mut cursor);
        }
        let cursor = Arc::new(Mutex::new(cursor));

        let cursor_id = ctx.app_context.save_cursor(cursor.clone());
        let cursor_doc = {
            let mut cursor_guard = cursor.lock().unwrap();
            FindHandler::mk_cursor_doc(cursor_id, db_name, &collection_name, &mut cursor_guard, batch_size as isize)?
        };

        let body = rawdoc! {
            "ok": 1,
            "cursor": cursor_doc,
        };
        let reply = Reply::new(ctx.message.response_to, body);
        debug!("find reply: {:?}", reply);
        Ok(reply)
    }
}
