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
    pub static KEY_TY_SHOULD_NOT_BE_ZERO: &str = "type name of KEY should not be zero";

}

pub mod validation_error_reason {

    pub static ILLEGAL_INDEX_OPTIONS_KEY: &str = "illegal key for index options";
    pub static TYPE_OF_INDEX_NAME_SHOULD_BE_STRING: &str = "type of index name should be string";
    pub static ORDER_OF_INDEX_CAN_ONLY_BE_ONE: &str = "order of index can only be one";
    pub static UNIQUE_PROP_SHOULD_BE_BOOLEAN: &str = "unique prop should be boolean";

}

#[derive(Debug)]
pub enum DbErr {
    NotAValidKeyType(String),
    ValidationError(String),
    ParseError(String),
    ParseIntError(num::ParseIntError),
    IOErr(io::Error),
    TypeNotComparable(String, String),
    DataSizeTooLarge(u32, u32),
    DecodeEOF,
    DecodeIntUnknownByte,
    DataOverflow,
    DataExist(Value),
    PageSpaceNotEnough,
    DataHasNoPrimaryKey,
    ChecksumMismatch,
    JournalPageSizeMismatch(u32, u32),
    SaltMismatch,
    PageMagicMismatch(u32),
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
            DbErr::NotAValidKeyType(ty_name) => write!(f, "type {} is not a valid key type", ty_name),
            DbErr::ValidationError(reason) => write!(f, "ValidationError: {}", reason),
            DbErr::ParseError(reason) => write!(f, "ParseError: {}", reason),
            DbErr::ParseIntError(parse_int_err) => std::fmt::Display::fmt(&parse_int_err, f),
            DbErr::IOErr(io_err) => std::fmt::Display::fmt(&io_err, f),
            DbErr::TypeNotComparable(expected, actual) =>
                write!(f, "TypeNotComparable(expected: {}, actual: {})", expected, actual),
            DbErr::DataSizeTooLarge(expected, actual) =>
                write!(f, "DataSizeTooLarge(expected: {}, actual: {})", expected, actual),
            DbErr::DecodeEOF => write!(f, "DecodeEOF"),
            DbErr::DecodeIntUnknownByte => write!(f, "DecodeIntUnknownByte"),
            DbErr::DataOverflow => write!(f, "DataOverflow"),
            DbErr::DataExist(value) => write!(f, "DataExist(pkey = {})", value.to_string()),
            DbErr::PageSpaceNotEnough => write!(f, "PageSpaceNotEnough"),
            DbErr::DataHasNoPrimaryKey => write!(f, "DataHasNoPrimaryKey"),
            DbErr::ChecksumMismatch => write!(f, "ChecksumMismatch"),
            DbErr::JournalPageSizeMismatch(expect, actual) => write!(f, "JournalPageSizeMismatch(expect={}, actual={})", expect, actual),
            DbErr::SaltMismatch => write!(f, "SaltMismatch"),
            DbErr::PageMagicMismatch(pid) => write!(f, "PageMagicMismatch({})", pid),
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
