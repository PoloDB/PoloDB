use std::rc::Rc;
use std::fmt;
use std::cmp::Ordering;

use super::ObjectId;
use super::document::Document;
use super::array::Array;
use crate::db::DbResult;
use crate::error::DbErr;

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Double(f64),
    Boolean(bool),

    // memory represent should use i64,
    // compress int when store on disk
    Int(i64),

    String(String),
    ObjectId(ObjectId),
    Array(Rc<Array>),
    Document(Rc<Document>),
}

impl Value {

    pub fn value_cmp(&self, other: &Value) -> DbResult<Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Ok(Ordering::Equal),
            (Value::Int(i1), Value::Int(i2)) => Ok(i1.cmp(i2)),
            (Value::String(str1), Value::String(str2)) => Ok(str1.cmp(str2)),
            (Value::ObjectId(oid1), Value::ObjectId(oid2)) => Ok(oid1.cmp(oid2)),
            _ =>
                return Err(DbErr::TypeNotComparable(self.ty_name().into(), other.ty_name().into()))
        }
    }

    #[inline]
    fn ty_name(&self) -> &str {
        match self {
            Value::Null        => "Null",
            Value::Double(_)   => "Double",
            Value::Boolean(_)  => "Boolean",
            Value::Int(_)      => "Int",
            Value::String(_)   => "String",
            Value::ObjectId(_) => "ObjectId",
            Value::Array(_)    => "Array",
            Value::Document(_) => "Document",
        }
    }

    #[inline]
    pub fn ty_int(&self) -> u8 {
        match self {
            Value::Null        => 0x0A,
            Value::Double(_)   => 0x01,
            Value::Boolean(_)  => 0x08,
            Value::Int(_)      => 0x16,
            Value::String(_)   => 0x02,
            Value::ObjectId(_) => 0x07,
            Value::Array(_)    => 0x17,
            Value::Document(_) => 0x13,
        }
    }

}

impl fmt::Display for Value {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "Null"),

            Value::Double(num) => write!(f, "Double({})", num),

            Value::Boolean(bl) => write!(f, "Boolean({})", bl),

            Value::Int(num) => write!(f, "Int({})", num),

            Value::String(str) => write!(f, "String({})", str),

            Value::ObjectId(oid) => write!(f, "ObjectId({})", oid),

            Value::Array(arr) => write!(f, "Array(len = {})", arr.len()),

            Value::Document(_) => write!(f, "Document(...)"),

        }
    }

}
