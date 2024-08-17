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

use core::fmt::Debug;
use bson::{doc, RawDocumentBuf};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use crate::wire::{Header, OpCode};

pub(crate) struct Reply {
    pub(crate) response_to: i32,
    doc: RawDocumentBuf,
    pub(crate) payload: Vec<u8>,
}

impl Reply {

    pub(crate) fn new(response_to: i32, doc: RawDocumentBuf) -> Reply {
        const SINGLE_DOC_KIND: u8 = 0;
        let mut payload = Vec::with_capacity(doc.as_bytes().len() + 5);
        std::io::Write::write_all(&mut payload, [SINGLE_DOC_KIND].as_slice()).unwrap();

        let doc_len = doc.as_bytes().len() as u32;
        std::io::Write::write_all(&mut payload, doc_len.to_le_bytes().as_slice()).unwrap();

        payload.extend_from_slice(doc.as_bytes());

        Reply { response_to, doc, payload }
    }

    /// Serializes the Header and writes the bytes to `w`.
    pub(crate) async fn write_to<W: AsyncWrite + Unpin>(&self, stream: &mut W) -> anyhow::Result<()> {
        let message_len = Header::LENGTH + self.payload.len();
        let header = Header {
            length: message_len as i32,
            request_id: 0,
            response_to: self.response_to,
            op_code: OpCode::Message,
        };
        header.write_to(stream).await?;
        stream.write_all(self.payload.as_slice()).await?;
        Ok(())
    }
}

impl Debug for Reply {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bson_doc = self.doc
            .to_document()
            .unwrap_or_else(|_| doc! { "error": "failed to serialize document" });
        write!(f, "Reply {{ response_to: {}, payload: {:?} }}", self.response_to, bson_doc)
    }

}
