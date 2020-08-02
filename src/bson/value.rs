
use super::ObjectId;
use super::document::Document;

#[derive(Debug, Clone)]
pub enum Value {
    Undefined,
    Double(f64),
    Boolean(bool),
    I32(i32),
    I64(i64),
    String(String),
    ObjectId(ObjectId),
    Document(Document),
}
