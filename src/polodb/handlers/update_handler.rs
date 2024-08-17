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
use anyhow::{anyhow, Result};
use crate::handlers::{HandleContext, Handler};
use async_trait::async_trait;
use bson::{rawdoc, Document, RawDocumentBuf};
use log::debug;
use polodb_core::results::UpdateResult;
use polodb_core::CollectionT;
use crate::app_context::AppContext;
use crate::reply::Reply;

pub(crate) struct UpdateHandler {}

impl UpdateHandler {

    pub(crate) fn new() -> Arc<dyn Handler> {
        Arc::new(UpdateHandler {})
    }

    fn handle_update(ctx: AppContext, col_name: &str, update: Document, result: &mut UpdateResult) -> Result<()> {
        let db = ctx.db();
        let collection = db.collection::<Document>(col_name);

        let filter = update.get("q").ok_or(anyhow!("update document missing q field"))?;
        let update = update.get("u").ok_or(anyhow!("update document missing u field"))?;

        let filter_doc = filter.as_document().ok_or(anyhow!("q field is not a document"))?;
        let update_doc = update.as_document().ok_or(anyhow!("u field is not a document"))?;

        let tmp_result = collection.update_many(filter_doc.clone(), update_doc.clone())?;
        result.modified_count += tmp_result.modified_count;

        Ok(())
    }

}

#[async_trait]
impl Handler for UpdateHandler {
    fn test(&self, doc: &RawDocumentBuf) -> anyhow::Result<bool> {
        let val = doc.get("update")?;
        match val {
            Some(r) => Ok(r.as_str().is_some()),
            None => Ok(false),
        }
    }

    async fn handle(&self, ctx: &HandleContext) -> Result<Reply> {
        let doc = &ctx.message.document_payload;
        let collection_name = doc.get("update")?.unwrap().as_str().ok_or(anyhow!("insert field is not a string"))?;

        let mut update_result = UpdateResult::default();

        let updates = doc.get_array("updates")?;
        for update in updates.into_iter() {
            let update = update?.as_document().ok_or(anyhow!("update is not a document"))?;
            let d = bson::from_slice::<Document>(update.as_bytes())?;
            UpdateHandler::handle_update(ctx.app_context.clone(), collection_name, d, &mut update_result)?;
        }
        debug!("update result: {:?}", update_result);

        let body = rawdoc! {
            "ok": 1,
            "connectionId": ctx.conn_id as i64,
            "nModified": update_result.modified_count as i64,
            "n": update_result.modified_count as i64,
        };
        let reply = Reply::new(ctx.message.request_id.unwrap(), body);
        Ok(reply)
    }
}
