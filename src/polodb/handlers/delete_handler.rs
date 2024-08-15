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
use crate::handlers::{HandleContext, Handler};
use async_trait::async_trait;
use bson::{rawdoc, Document, RawDocumentBuf};
use anyhow::{anyhow, Result};
use polodb_core::results::DeleteResult;
use crate::app_context::AppContext;
use crate::reply::Reply;

pub(crate) struct DeleteHandler {}

impl DeleteHandler {

    pub(crate) fn new() -> Arc<dyn Handler> {
        Arc::new(DeleteHandler {})
    }

    fn handle_delete(ctx: AppContext, col_name: &str, delete_doc: Document, result: &mut DeleteResult) -> Result<()> {
        let db = ctx.db();
        let collection = db.collection::<Document>(col_name);

        let filter = delete_doc.get_document("q")?;
        let tmp_result = collection.delete_many(filter.clone())?;

        result.deleted_count += tmp_result.deleted_count;

        Ok(())
    }

}

#[async_trait]
impl Handler for DeleteHandler {
    fn test(&self, doc: &RawDocumentBuf) -> Result<bool> {
        let val = doc.get("delete")?;
        match val {
            Some(r) => Ok(r.as_str().is_some()),
            None => Ok(false),
        }
    }

    async fn handle(&self, ctx: &HandleContext) -> Result<Reply> {
        let doc = &ctx.message.document_payload;
        let collection_name = doc.get("delete")?.unwrap().as_str().ok_or(anyhow!("delete field is not a string"))?;

        let deletes_arr = doc.get("deletes")?.unwrap().as_array().ok_or(anyhow!("deletes field is not an array"))?;
        let mut delete_result = DeleteResult::default();

        for delete_doc in deletes_arr.into_iter() {
            let doc_ref = delete_doc?.as_document().ok_or(anyhow!("delete document is not a document"))?;
            let doc = bson::from_slice(doc_ref.as_bytes())?;
            DeleteHandler::handle_delete(ctx.app_context.clone(), collection_name, doc, &mut delete_result)?;
        }

        let body = rawdoc! {
            "ok": 1,
            "n": delete_result.deleted_count as i64,
        };
        let reply = Reply::new(ctx.message.request_id.unwrap(), body);
        Ok(reply)
    }
}
