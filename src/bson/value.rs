
use super::ObjectId;
use super::document::Document;

#[derive(Debug)]
pub enum Value {
    Double(f64),
    Boolean(bool),
    I32(i32),
    I64(i64),
    String(String),
    ObjectId(ObjectId),
    Document(Document),
}
