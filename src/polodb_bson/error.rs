use std::fmt;
use std::num;
use std::io;
use std::str::Utf8Error;

#[derive(Debug)]
pub enum BsonErr {
    ParseError(String),
    ParseIntError(Box<num::ParseIntError>),
    DecodeIntUnknownByte,
    IOErr(Box<io::Error>),
    UTF8Error(Box<Utf8Error>),
    TypeNotComparable(String, String),
}

pub mod parse_error_reason {

    pub static OBJECT_ID_LEN: &str = "length of ObjectId should be 12";
    pub static OBJECT_ID_HEX_DECODE_ERROR: &str = "decode error failed for ObjectID";
    pub static UNEXPECTED_DOCUMENT_FLAG: &str = "unexpected flag for document";
    pub static UNEXPECTED_PAGE_HEADER: &str = "unexpected page header";
    pub static UNEXPECTED_PAGE_TYPE: &str = "unexpected page type";
    pub static UNEXPECTED_HEADER_FOR_BTREE_PAGE: &str = "unexpected header for btree page";
    pub static KEY_TY_SHOULD_NOT_BE_ZERO: &str = "type name of KEY should not be zero";

}

impl fmt::Display for BsonErr {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BsonErr::ParseError(reason) => write!(f, "ParseError: {}", reason),
            BsonErr::ParseIntError(parse_int_err) => parse_int_err.fmt(f),
            BsonErr::DecodeIntUnknownByte => write!(f, "DecodeIntUnknownByte"),
            BsonErr::IOErr(io_err) => std::fmt::Display::fmt(&io_err, f),
            BsonErr::UTF8Error(utf8_err) => std::fmt::Display::fmt(&utf8_err, f),
            BsonErr::TypeNotComparable(expected, actual) =>
                write!(f, "TypeNotComparable(expected: {}, actual: {})", expected, actual),
        }
    }

}

impl From<io::Error> for BsonErr {

    fn from(error: io::Error) -> Self {
        BsonErr::IOErr(Box::new(error))
    }

}

impl From<Utf8Error> for BsonErr {

    fn from(error: Utf8Error) -> Self {
        BsonErr::UTF8Error(Box::new(error))
    }

}

impl From<num::ParseIntError> for BsonErr {

    fn from(error: num::ParseIntError) -> Self {
        BsonErr::ParseIntError(Box::new(error))
    }

}
