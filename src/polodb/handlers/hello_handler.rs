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
use bson::{rawdoc, RawDocumentBuf};
use anyhow::Result;
use crate::app_context::AppContext;
use crate::handlers::Handler;
use crate::reply::Reply;
use crate::wire;
use async_trait::async_trait;
use log::debug;

pub(crate) struct HelloHandler {}

impl HelloHandler {

    pub(crate) fn new() -> Arc<dyn Handler> {
        Arc::new(HelloHandler {})
    }

}

#[async_trait]
impl Handler for HelloHandler {

    fn test(&self, doc: &RawDocumentBuf) -> Result<bool> {
        let val = doc.get("helloOk")?;
        Ok(val.is_some())
    }

    async fn handle(&self, ctx: AppContext, conn_id: u64, message: &wire::Message) -> Result<Reply> {
        debug!("HelloHandler::handle {}", message.request_id.unwrap());
        let body = rawdoc! {
            "ok": 1,
            "connectionId": conn_id as i64,
            "minWireVersion": 6,
            "maxWireVersion": 21,
            "maxBsonObjectSize": 16 * 1024 * 1024,
            "maxMessageSizeBytes": 48000000,
        };
        let reply = Reply::new(message.request_id.unwrap(), body);
        Ok(reply)
    }

}
