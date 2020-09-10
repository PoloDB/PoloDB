use std::rc::Rc;
use std::fmt;
use std::cmp::Ordering;

use super::ObjectId;
use super::document::Document;
use super::array::Array;
use super::hex;
use crate::db::DbResult;
use crate::error::DbErr;

#[inline]
pub fn mk_object_id(content: &ObjectId) -> Value {
    Value::ObjectId(Rc::new(content.clone()))
}

#[inline]
pub fn mk_str(content: &str) -> Value {
    Value::String(Rc::new(content.into()))
}

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Double(f64),
    Boolean(bool),

    // memory represent should use i64,
    // compress int when store on disk
    Int(i64),

    String(Rc<String>),
    ObjectId(Rc<ObjectId>),
    Array(Rc<Array>),
    Document(Rc<Document>),

    Binary(Rc<Vec<u8>>),
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

    pub fn ty_name(&self) -> &str {
        match self {
            Value::Null        => "Null",
            Value::Double(_)   => "Double",
            Value::Boolean(_)  => "Boolean",
            Value::Int(_)      => "Int",
            Value::String(_)   => "String",
            Value::ObjectId(_) => "ObjectId",
            Value::Array(_)    => "Array",
            Value::Document(_) => "Document",
            Value::Binary(_)   => "Binary",
        }
    }

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
            Value::Binary(_)   => 0x05,
        }
    }

    #[inline]
    pub fn unwrap_document(&self) -> &Rc<Document> {
        match self {
            Value::Document(doc) => doc,
            _ => panic!("unwrap error: document expected, but it's {}", self.ty_name()),
        }
    }

    #[inline]
    pub fn unwrap_boolean(&self) -> bool {
        match self {
            Value::Boolean(bl) => *bl,
            _ => panic!("unwrap error: boolean expected, but it's {}", self.ty_name()),
        }
    }

    #[inline]
    pub fn unwrap_int(&self) -> i64 {
        match self {
            Value::Int(i) => *i,
            _ => panic!("unwrap error: int expected, but it's {}", self.ty_name()),
        }
    }

    #[inline]
    pub fn unwrap_string(&self) -> &str {
        match self {
            Value::String(str) => str,
            _ => panic!("unwrap error: string expected, but it's {}", self.ty_name()),
        }
    }

    pub fn is_valid_key_type(&self) -> bool {
        match self {
            Value::String(_) |
            Value::Int(_) |
            Value::ObjectId(_) |
            Value::Boolean(_) => true,

            _ => false

        }
    }

}

impl fmt::Display for Value {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "Null"),

            Value::Double(num) => write!(f, "Double({})", num),

            Value::Boolean(bl) => if *bl {
                write!(f, "true")
            } else {
                write!(f, "false")
            },

            Value::Int(num) => write!(f, "{}", num),

            Value::String(str) => write!(f, "\"{}\"", str),

            Value::ObjectId(oid) => write!(f, "ObjectId({})", oid),

            Value::Array(arr) => write!(f, "Array(len = {})", arr.len()),

            Value::Document(_) => write!(f, "Document(...)"),

            Value::Binary(bin) => {
                if bin.len() > 64 {
                    return write!(f, "Binary(...)");
                }

                let hex_string_content = hex::encode(bin.as_ref());
                write!(f, "Binary({})", hex_string_content)
            }

        }
    }

}
