use std::rc::Rc;
use std::fmt;
use std::cmp::Ordering;
use super::ObjectId;
use super::document::Document;
use super::array::Array;
use super::hex;
use crate::BsonResult;
use crate::error::BsonErr;
use crate::datetime::UTCDateTime;

const BINARY_MAX_DISPLAY_LEN: usize = 64;

#[inline]
pub fn mk_object_id(content: &ObjectId) -> Value {
    Value::ObjectId(Rc::new(content.clone()))
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

    UTCDateTime(Rc<UTCDateTime>),

}

impl Value {

    pub fn value_cmp(&self, other: &Value) -> BsonResult<Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Ok(Ordering::Equal),
            (Value::Int(i1), Value::Int(i2)) => Ok(i1.cmp(i2)),
            (Value::String(str1), Value::String(str2)) => Ok(str1.cmp(str2)),
            (Value::ObjectId(oid1), Value::ObjectId(oid2)) => Ok(oid1.cmp(oid2)),
            _ => Err(BsonErr::TypeNotComparable(self.ty_name().into(), other.ty_name().into())),
        }
    }

    pub fn ty_name(&self) -> &'static str {
        match self {
            Value::Null           => "Null",
            Value::Double(_)      => "Double",
            Value::Boolean(_)     => "Boolean",
            Value::Int(_)         => "Int",
            Value::String(_)      => "String",
            Value::ObjectId(_)    => "ObjectId",
            Value::Array(_)       => "Array",
            Value::Document(_)    => "Document",
            Value::Binary(_)      => "Binary",
            Value::UTCDateTime(_) => "UTCDateTime",
        }
    }

    pub fn ty_int(&self) -> u8 {
        match self {
            Value::Null           => ty_int::NULL,
            Value::Double(_)      => ty_int::DOUBLE,
            Value::Boolean(_)     => ty_int::BOOLEAN,
            Value::Int(_)         => ty_int::INT,
            Value::String(_)      => ty_int::STRING,
            Value::ObjectId(_)    => ty_int::OBJECT_ID,
            Value::Array(_)       => ty_int::ARRAY,
            Value::Document(_)    => ty_int::DOCUMENT,
            Value::Binary(_)      => ty_int::BINARY,
            Value::UTCDateTime(_) => ty_int::UTC_DATETIME,

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
    pub fn unwrap_array(&self) -> &Rc<Array> {
        match self {
            Value::Array(arr) => arr,
            _ => panic!("unwrap error: document expected, but it's {}", self.ty_name()),
        }
    }

    #[inline]
    pub fn unwrap_document_mut(&mut self) -> &mut Rc<Document> {
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

    #[inline]
    pub fn unwrap_binary(&self) -> &Rc<Vec<u8>> {
        match self {
            Value::Binary(bin) => bin,
            _ => panic!("unwrap error: binary expected, but it's {}", self.ty_name()),
        }
    }

    pub fn is_valid_key_type(&self) -> bool {
        matches!(self, Value::String(_) |
                       Value::Int(_) |
                       Value::ObjectId(_) |
                       Value::Boolean(_))
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

            Value::Array(arr) => write!(f, "Array(len={})", arr.len()),

            Value::Document(doc) => write!(f, "Document(len={}, ...)", doc.len()),

            Value::Binary(bin) => {
                if bin.len() > BINARY_MAX_DISPLAY_LEN {
                    return write!(f, "Binary(...)");
                }

                let hex_string_content = hex::encode(bin.as_ref());
                write!(f, "Binary({})", hex_string_content)
            }

            Value::UTCDateTime(datetime) => {
                write!(f, "UTCDateTime({})", datetime.timestamp())
            }

        }
    }

}

pub mod ty_int {
    pub const NULL: u8         = 0x0A;
    pub const DOUBLE: u8       = 0x01;
    pub const BOOLEAN: u8      = 0x08;
    pub const INT: u8          = 0x16;
    pub const STRING: u8       = 0x02;
    pub const OBJECT_ID: u8    = 0x07;
    pub const ARRAY: u8        = 0x17;
    pub const DOCUMENT: u8     = 0x13;
    pub const BINARY: u8       = 0x05;
    pub const UTC_DATETIME: u8 = 0x09;

    pub fn to_str(i: u8) -> &'static str {
        match i {
            NULL => "Null",
            BOOLEAN => "Boolean",
            INT => "Int",
            STRING => "String",
            OBJECT_ID => "ObjectId",
            ARRAY => "Array",
            DOCUMENT => "Document",
            BINARY => "Binary",
            UTC_DATETIME => "UTCDateTime",

            _ => "<unknown>"
        }
    }

}

impl From<i32> for Value {

    fn from(int: i32) -> Self {
        Value::Int(int as i64)
    }

}

impl From<u32> for Value {

    fn from(int: u32) -> Self {
        Value::Int(int as i64)
    }

}

impl From<i64> for Value {

    fn from(int: i64) -> Self {
        Value::Int(int)
    }

}

impl From<u64> for Value {

    fn from(int: u64) -> Self {
        Value::Int(int as i64)
    }

}

impl From<usize> for Value {

    fn from(int: usize) -> Self {
        Value::Int(int as i64)
    }

}

impl From<&str> for Value {

    fn from(string: &str) -> Self {
        let str: Rc<String> = Rc::new(string.into());
        Value::String(str)
    }

}

impl From<String> for Value {

    fn from(string: String) -> Self {
        Value::String(Rc::new(string))
    }

}

impl From<Rc<String>> for Value {

    fn from(str: Rc<String>) -> Self {
        Value::String(str)
    }

}

impl From<bool> for Value {

    fn from(bl: bool) -> Self {
        Value::Boolean(bl)
    }

}

impl From<f64> for Value {

    fn from(float: f64) -> Self {
        Value::Double(float)
    }

}

impl From<ObjectId> for Value {

    fn from(oid: ObjectId) -> Self {
        Value::ObjectId(Rc::new(oid))
    }

}

impl From<Document> for Value {

    fn from(doc: Document) -> Self {
        Value::Document(Rc::new(doc))
    }

}

impl From<Array> for Value {

    fn from(arr: Array) -> Self {
        Value::Array(Rc::new(arr))
    }

}

impl From<Vec<u8>> for Value {

    fn from(buf: Vec<u8>) -> Self {
        Value::Binary(Rc::new(buf))
    }

}

impl From<UTCDateTime> for Value {

    fn from(datetime: UTCDateTime) -> Self {
        Value::UTCDateTime(Rc::new(datetime))
    }

}
