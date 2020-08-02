
use std::fmt;
use super::ObjectId;
use super::document::Document;

#[derive(Debug, Clone)]
pub enum Value {
    Undefined,
    Double(f64),
    Boolean(bool),

    // memory represent should use i64,
    // compress int when store on disk
    Int(i64),

    String(String),
    ObjectId(ObjectId),
    Document(Document),
}

impl fmt::Display for Value {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Undefined => write!(f, "Undefined"),

            Value::Double(num) => write!(f, "Double({})", num),

            Value::Boolean(bl) => write!(f, "Boolean({})", bl),

            Value::Int(num) => write!(f, "Int({})", num),

            Value::String(str) => write!(f, "String({})", str),

            Value::ObjectId(oid) => write!(f, "ObjectId({})", oid),

            Value::Document(_) => write!(f, "Document(...)"),

        }
    }

}
