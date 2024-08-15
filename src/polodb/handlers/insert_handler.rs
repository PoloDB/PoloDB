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

use std::sync::Arc;
use anyhow::anyhow;
use bson::{rawdoc, RawDocumentBuf};
use crate::handlers::{HandleContext, Handler};
use crate::reply::Reply;
use async_trait::async_trait;
use tokio::task;
use log::debug;

pub(crate) struct InsertHandler {}

impl InsertHandler {

    pub(crate) fn new() -> Arc<dyn Handler> {
        Arc::new(InsertHandler {})
    }

}

#[async_trait]
impl Handler for InsertHandler {

    fn test(&self, doc: &RawDocumentBuf) -> anyhow::Result<bool> {
        let val = doc.get("insert")?;
        match val {
            Some(r) => Ok(r.as_str().is_some()),
            None => Ok(false),
        }
    }
    async fn handle(&self, ctx: &HandleContext) -> anyhow::Result<Reply> {
        let doc = &ctx.message.document_payload;
        let collection_name = doc.get("insert")?.unwrap().as_str().ok_or(anyhow!("insert field is not a string"))?;
        let db = ctx.app_context.db();
        let collection = db.collection::<bson::Document>(collection_name);

        let mut batch_insert = Vec::<bson::Document>::new();
        for doc_seq in ctx.message.document_sequences.as_slice() {
            for doc in doc_seq.documents.as_slice() {
                let d = bson::from_slice::<bson::Document>(doc.as_bytes())?;
                batch_insert.push(d);
            }
        }

        // insert could be blocking, so we spawn a blocking task
        let insert_result = task::spawn_blocking(move || {
            collection.insert_many(batch_insert.as_slice())
        }).await??;
        debug!("inserted {} documents", insert_result.inserted_ids.len());

        let body = rawdoc! {
            "ok": 1,
            "connectionId": ctx.conn_id as i64,
            "n": insert_result.inserted_ids.len() as i64,
        };
        let reply = Reply::new(ctx.message.request_id.unwrap(), body);
        Ok(reply)
    }

}
