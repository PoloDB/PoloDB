use bson::Document;
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
            let bytes = bson::ser::to_vec(&doc)?;
            buf.write(&bytes)?;
        },
        SerializeType::Legacy => {
            let bytes = bson::ser::to_vec(&doc)?;
            buf.write(&bytes)?;
        }
    }
    Ok(())
}

pub(crate) fn deserialize(ty: SerializeType, buf: &mut &[u8]) -> DbResult<Document> {
    match ty {
        SerializeType::Default => {
            let doc = bson::from_slice(buf)?;
            Ok(doc)
        },
        SerializeType::Legacy => {
            let doc = bson::from_slice(buf)?;
            Ok(doc)
        },
    }
}
