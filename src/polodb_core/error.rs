use std::io;
use std::fmt;
use std::num;
use crate::bson::Value;

pub mod parse_error_reason {

    pub static OBJECT_ID_LEN: &str = "length of ObjectId should be 12";
    pub static OBJECT_ID_HEX_DECODE_ERROR: &str = "decode error failed for ObjectID";
    pub static UNEXPECTED_DOCUMENT_FLAG: &str = "unexpected flag for document";
    pub static UNEXPECTED_PAGE_HEADER: &str = "unexpcted page header";
    pub static UNEXPECTED_PAGE_TYPE: &str = "unexpected page type";
    pub static UNEXPECTED_HEADER_FOR_BTREE_PAGE: &str = "unexpected header for btree page";

}

#[derive(Debug)]
pub enum DbErr {
    ParseError(String),
    ParseIntError(num::ParseIntError),
    IOErr(io::Error),
    TypeNotComparable(String, String),
    NotImplement,
    DecodeEOF,
    DecodeIntUnknownByte,
    DataOverflow,
    DataExist(Value),
    PageSpaceNotEnough,
    DataHasNoPrimaryKey,
    ChecksumMismatch,
    JournalPageSizeMismatch(u32, u32),
    SaltMismatch,
    ItemSizeGreaterThenExpected,
    CollectionNotFound(String),
    MetaPageIdError,
    CannotWriteDbWithoutTransaction,
    StartTransactionInAnotherTransaction,
    RollbackNotInTransaction,
    Busy
}

impl fmt::Display for DbErr {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DbErr::ParseError(reason) => write!(f, "ParseError: {}", reason),
            DbErr::ParseIntError(parse_int_err) => std::fmt::Display::fmt(&parse_int_err, f),
            DbErr::IOErr(io_err) => std::fmt::Display::fmt(&io_err, f),
            DbErr::TypeNotComparable(expected, actual) =>
                write!(f, "TypeNotComparable(expected: {}, actual: {})", expected, actual),
            DbErr::NotImplement => write!(f, "NotImplement"),
            DbErr::DecodeEOF => write!(f, "DecodeEOF"),
            DbErr::DecodeIntUnknownByte => write!(f, "DecodeIntUnknownByte"),
            DbErr::DataOverflow => write!(f, "DataOverflow"),
            DbErr::DataExist(value) => write!(f, "DataExist(pkey = {})", value.to_string()),
            DbErr::PageSpaceNotEnough => write!(f, "PageSpaceNotEnough"),
            DbErr::DataHasNoPrimaryKey => write!(f, "DataHasNoPrimaryKey"),
            DbErr::ChecksumMismatch => write!(f, "ChecksumMismatch"),
            DbErr::JournalPageSizeMismatch(expect, actual) => write!(f, "JournalPageSizeMismatch(expect={}, actual={})", expect, actual),
            DbErr::SaltMismatch => write!(f, "SaltMismatch"),
            DbErr::ItemSizeGreaterThenExpected => write!(f, "ItemSizeGreaterThenExpected"),
            DbErr::CollectionNotFound(name) => write!(f, "collection \"{}\" not found", name),
            DbErr::MetaPageIdError => write!(f, "meta page id should not be zero"),
            DbErr::CannotWriteDbWithoutTransaction => write!(f, "cannot write Db without transaction"),
            DbErr::StartTransactionInAnotherTransaction => write!(f, "start transaction in another transaction"),
            DbErr::RollbackNotInTransaction => write!(f, "can not rollback because not int transaction"),
            DbErr::Busy => write!(f, "database busy"),
        }
    }

}

impl From<io::Error> for DbErr {

    fn from(error: io::Error) -> Self {
        DbErr::IOErr(error)
    }

}

impl From<num::ParseIntError> for DbErr {

    fn from(error: num::ParseIntError) -> Self {
        DbErr::ParseIntError(error)
    }

}
