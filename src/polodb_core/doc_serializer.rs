use polodb_bson::Document;
use crate::DbResult;
use std::io::Write;

#[derive(Eq, PartialEq, Copy, Clone)]
pub enum SerializeType {
    Default,
    Legacy
}

pub(crate) fn serialize(ty: SerializeType, doc: &Document, buf: &mut Vec<u8>) -> DbResult<()> {
    match ty {
        SerializeType::Default => {
            doc.to_msgpack(buf)?;
        },
        SerializeType::Legacy => {
            let tmp = doc.to_bytes()?;
            buf.write(&tmp)?;
        }
    }
    Ok(())
}

pub(crate) fn deserialize(ty: SerializeType, buf: &mut &[u8]) -> DbResult<Document> {
    match ty {
        SerializeType::Default => {
            let doc = Document::from_msgpack(buf)?;
            Ok(doc)
        },
        SerializeType::Legacy => {
            let doc = Document::from_bytes(buf)?;
            Ok(doc)
        },
    }
}
