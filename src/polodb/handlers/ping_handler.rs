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

use crate::handlers::{HandleContext, Handler};
use crate::reply::Reply;
use anyhow::Result;
use async_trait::async_trait;
use bson::{rawdoc, RawDocumentBuf};
use log::debug;
use std::sync::Arc;

pub(crate) struct PingHandler {}

impl PingHandler {
    pub(crate) fn new() -> Arc<dyn Handler> {
        Arc::new(PingHandler {})
    }
}

#[async_trait]
impl Handler for PingHandler {
    fn test(&self, doc: &RawDocumentBuf) -> Result<bool> {
        let val = doc.get("ping")?;
        Ok(val.is_some())
    }

    async fn handle(&self, ctx: &HandleContext) -> Result<Reply> {
        let req_id = ctx.message.request_id.unwrap();
        debug!("PingHandler::handle {}", req_id);
        let body = rawdoc! {
            "ok": 1
        };
        let reply = Reply::new(req_id, body);
        Ok(reply)
    }
}
