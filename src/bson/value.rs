
use std::fmt;
use super::ObjectId;
use super::document::Document;
use super::array::Array;
use crate::vli;
use crate::serialization::DbSerializer;

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
    Array(Array),
    Document(Document),
}

impl Value {

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
    fn ty_int(&self) -> u8 {
        match self {
            Value::Null        => 0,
            Value::Double(_)   => 1,
            Value::Boolean(_)  => 2,
            Value::Int(_)      => 3,
            Value::String(_)   => 4,
            Value::ObjectId(_) => 5,
            Value::Array(_)    => 6,
            Value::Document(_) => 7,
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

// impl DbSerializer for Value {
//
//     fn serialize(&self, writer: &mut dyn Write) -> DbResult<()> {
//         let ty8: u8 = self.ty_int();
//
//         writer.write_all(&[ ty8 ])?;
//
//         match self {
//             Value::Undefined => (),
//
//             Value::Double(value) => {
//                 let buffer = value.to_be_bytes();
//                 writer.write_all(&buffer)?;
//             }
//
//             Value::Boolean(value) => {
//                 let v8: u8 = if *value { 1 } else { 0 };
//                 writer.write_all(&[ v8 ])?;
//             }
//
//             Value::Int(int_val) => {
//                 vli::encode(writer, *int_val)?;
//             }
//
//             Value::String(str) => {
//                 let len = str.len();
//                 vli::encode(writer, len as i64)?;
//                 writer.write_all(str.as_bytes())?;
//             }
//
//             Value::ObjectId(oid) => {
//                 oid.serialize(writer)?;
//             }
//
//             Value::Array(arr) => {
//                 let len = arr.len();
//                 vli::encode(writer, len as i64)?;
//
//                 for item in &arr.data {
//                     item.serialize(writer)?;
//                 }
//             }
//
//             Value::Document(doc) => {
//                 doc.serialize(writer)?;
//             }
//         }
//
//         Ok(())
//     }
//
// }
