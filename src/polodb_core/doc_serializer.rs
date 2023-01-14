use bson::Document;
use crate::DbResult;
use std::io::Write;

pub(crate) fn serialize(doc: &Document, buf: &mut Vec<u8>) -> DbResult<()> {
    let bytes = bson::ser::to_vec(&doc)?;
    buf.write(&bytes)?;
    Ok(())
}

pub(crate) fn deserialize(buf: &mut &[u8]) -> DbResult<Document> {
    let doc = bson::from_slice(buf)?;
    Ok(doc)
}
