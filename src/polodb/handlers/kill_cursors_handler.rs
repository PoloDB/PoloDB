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
use anyhow::{anyhow, Result};
use bson::{rawdoc, RawDocumentBuf};
use crate::reply::Reply;

pub(crate) struct KillCursorsHandler {}

impl KillCursorsHandler {

    pub fn new() -> Arc<dyn Handler> {
        Arc::new(KillCursorsHandler {})
    }

}

#[async_trait]
impl Handler for KillCursorsHandler {
    fn test(&self, doc: &RawDocumentBuf) -> Result<bool> {
        let val = doc.get("killCursors")?;
        Ok(val.is_some())
    }

    async fn handle(&self, ctx: &HandleContext) -> Result<Reply> {
        let doc = &ctx.message.document_payload;
        let _collection_name = doc.get("killCursors")?.unwrap().as_str().ok_or(anyhow!("insert field is not a string"))?;
        let cursors_array = match doc.get("cursors")? {
            Some(val) => {
                val.as_array().ok_or(anyhow!("cursorsArray is not an array"))?
            },
            None => {
                return Err(anyhow!("cursorsArray is missing"));
            },
        };

        let mut cursor_ids = Vec::<i64>::new();
        for cursor_id in cursors_array.into_iter() {
            let id = cursor_id?.as_i64().ok_or(anyhow!("cursor id is not an i64"))?;
            cursor_ids.push(id);
        }
        ctx.app_context.remove_cursor(cursor_ids.as_slice());

        let body = rawdoc! {
            "ok": 1,
        };
        let reply = Reply::new(ctx.message.request_id.unwrap(), body);
        Ok(reply)
    }
}
