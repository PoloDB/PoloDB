use std::rc::Rc;
use std::fmt;
use std::cmp::Ordering;
use std::io::Read;
use rmp::Marker;
use super::ObjectId;
use super::document::Document;
use super::array::Array;
use super::hex;
use crate::BsonResult;
use crate::error::BsonErr;
use crate::datetime::UTCDateTime;
use byteorder::{self, ReadBytesExt, BigEndian};

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

    pub fn to_msgpack(&self, buf: &mut Vec<u8>) -> BsonResult<()> {
        match self {
            Value::Null => rmp::encode::write_nil(buf)?,
            Value::Double(fv) => {
                if *fv <= (f32::MAX as f64) && *fv >= (f32::MIN as f64) {
                    rmp::encode::write_f32(buf, *fv as f32)?;
                } else {
                    rmp::encode::write_f64(buf, *fv)?;
                }
            },
            Value::Boolean(bv) => rmp::encode::write_bool(buf, *bv)?,
            Value::Int(iv) => {
                let v = *iv;
                if v <= (i8::MAX as i64) && v >= (i8::MIN as i64) {
                    rmp::encode::write_i8(buf, v as i8)?
                } else if v <= (i16::MAX as i64) && v >= (i16::MIN as i64) {
                    rmp::encode::write_i16(buf, v as i16)?;
                } else if v <= (i32::MAX as i64) && v >= (i32::MIN as i64) {
                    rmp::encode::write_i32(buf, v as i32)?;
                } else {
                    rmp::encode::write_i64(buf, v)?;
                }
            }
            Value::String(str) => {
                rmp::encode::write_str(buf, str)?;
            },
            Value::ObjectId(oid) => {
                rmp::encode::write_ext_meta(buf, 1, ty_int::OBJECT_ID as i8)?;
                oid.serialize(buf)?;
            },
            Value::Array(arr) => {
                arr.to_msgpack(buf)?;
            },
            Value::Document(doc) => {
                doc.to_msgpack(buf)?;
            },
            Value::Binary(bin) => {
                rmp::encode::write_bin(buf, bin)?;
            },
            Value::UTCDateTime(_) => {
                unimplemented!()
            },
        }
        Ok(())
    }

    pub fn from_msgpack<R: Read>(bytes: &mut R) -> BsonResult<Value> {
        let marker = rmp::decode::read_marker(bytes)?;
        match marker {
            Marker::Null => {
                Ok(Value::Null)
            }
            Marker::True => {
                Ok(Value::Boolean(true))
            }
            Marker::False => {
                Ok(Value::Boolean(false))
            }
            Marker::FixPos(int) => {
                Ok(Value::Int(int as i64))
            }
            Marker::U8 => {
                let b = bytes.read_u8()?;
                Ok(Value::Int(b as i64))
            }
            Marker::U16 => {
                let v = bytes.read_u16::<BigEndian>()?;
                Ok(Value::Int(v as i64))
            }
            Marker::U32 => {
                let v = bytes.read_u32::<BigEndian>()?;
                Ok(Value::Int(v as i64))
            }
            Marker::U64 => {
                let v = bytes.read_u64::<BigEndian>()?;
                Ok(Value::Int(v as i64))
            }
            Marker::I8 => {
                let v = bytes.read_i8()?;
                Ok(Value::Int(v as i64))
            }
            Marker::I16 => {
                let v = bytes.read_i16::<BigEndian>()?;
                Ok(Value::Int(v as i64))
            }
            Marker::I32 => {
                let v = bytes.read_i32::<BigEndian>()?;
                Ok(Value::Int(v as i64))
            }
            Marker::I64 => {
                let v = bytes.read_i64::<BigEndian>()?;
                Ok(Value::Int(v))
            }
            Marker::F32 => {
                let v = bytes.read_f32::<BigEndian>()?;
                Ok(Value::Double(v as f64))
            }
            Marker::F64 => {
                let v = bytes.read_f64::<BigEndian>()?;
                Ok(Value::Double(v))
            }
            Marker::FixStr(size) => {
                let len = size as usize;

                let mut buf = vec![0u8; len];
                let _ = bytes.read(&mut buf)?;

                let str = String::from_utf8(buf)?;
                Ok(str.into())
            }
            Marker::Str8 => {
                let len = bytes.read_u8()? as usize;

                let mut buf = vec![0u8; len];
                let _ = bytes.read(&mut buf)?;

                let str = String::from_utf8(buf)?;
                Ok(str.into())
            }
            Marker::Str16 => {
                let len = bytes.read_u16::<BigEndian>()? as usize;

                let mut buf = vec![0u8; len];
                let _ = bytes.read(&mut buf)?;

                let str = String::from_utf8(buf)?;
                Ok(str.into())
            }
            Marker::Str32 => {
                let len = bytes.read_u32::<BigEndian>()? as usize;

                let mut buf = vec![0u8; len];
                let _ = bytes.read(&mut buf)?;

                let str = String::from_utf8(buf)?;
                Ok(str.into())
            }
            Marker::Bin8 => {
                let len = bytes.read_u8()? as usize;

                let mut buf = vec![0u8; len];
                let _ = bytes.read(&mut buf)?;

                Ok(buf.into())
            }
            Marker::Bin16 => {
                let len = bytes.read_u16::<BigEndian>()? as usize;

                let mut buf = vec![0u8; len];
                let _ = bytes.read(&mut buf)?;

                Ok(buf.into())
            }
            Marker::Bin32 => {
                let len = bytes.read_u32::<BigEndian>()? as usize;

                let mut buf = vec![0u8; len];
                let _ = bytes.read(&mut buf)?;

                Ok(buf.into())
            }
            Marker::FixArray(size) => {
                let len = size as usize;
                let arr = Array::from_msgpack_with_len(bytes, len)?;
                Ok(Value::Array(Rc::new(arr)))
            }
            Marker::Array16 => {
                let len = bytes.read_u16::<BigEndian>()? as usize;
                let arr = Array::from_msgpack_with_len(bytes, len)?;
                Ok(Value::Array(Rc::new(arr)))
            }
            Marker::Array32 => {
                let len = bytes.read_u32::<BigEndian>()? as usize;
                let arr = Array::from_msgpack_with_len(bytes, len)?;
                Ok(Value::Array(Rc::new(arr)))
            }
            Marker::FixMap(size) => {
                let len = size as usize;
                let doc = Document::from_msgpack_with_len(bytes, len)?;
                Ok(Value::Document(Rc::new(doc)))
            }
            Marker::Map16 => {
                let len = bytes.read_u16::<BigEndian>()? as usize;
                let doc = Document::from_msgpack_with_len(bytes, len)?;
                Ok(Value::Document(Rc::new(doc)))
            }
            Marker::Map32 => {
                let len = bytes.read_u32::<BigEndian>()? as usize;
                let doc = Document::from_msgpack_with_len(bytes, len)?;
                Ok(Value::Document(Rc::new(doc)))
            }
            Marker::FixExt1 => {
                let ty = bytes.read_u8()?;
                if ty == ty_int::OBJECT_ID {
                    let mut buf = [0; 12];
                    bytes.read(&mut buf)?;
                    let oid = ObjectId::deserialize(&buf)?;
                    Ok(Value::ObjectId(Rc::new(oid)))
                } else {
                    Err(BsonErr::ParseError("unknown ext type".into()))
                }
            }
            _ => {
                Err(BsonErr::ParseError("unexpected meta".into()))
            }
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
