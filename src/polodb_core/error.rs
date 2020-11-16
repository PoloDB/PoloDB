use std::io;
use std::fmt;
use polodb_bson::{Value, ty_int};
use polodb_bson::error::BsonErr;

#[derive(Debug)]
pub struct FieldTypeUnexpectedStruct {
    pub field_name: String,
    pub expected_ty: String,
    pub actual_ty: String,
}

impl fmt::Display for FieldTypeUnexpectedStruct {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "unexpected type for field '{}', expected: {}, actual: {}",
               self.field_name, self.expected_ty, self.actual_ty)
    }

}

#[inline]
pub(crate) fn mk_field_name_type_unexpected(option_name: &str, expected_ty: &str, actual_ty: &str) -> DbErr {
    DbErr::FieldTypeUnexpected(Box::new(FieldTypeUnexpectedStruct {
        field_name: option_name.into(),
        expected_ty: expected_ty.into(),
        actual_ty: actual_ty.into(),
    }))
}

#[derive(Debug)]
pub enum DbErr {
    UnexpectedIdType(u8, u8),
    NotAValidKeyType(String),
    NotAValidField(String),
    ValidationError(String),
    InvalidOrderOfIndex(String),
    IndexAlreadyExists(String),
    FieldTypeUnexpected(Box<FieldTypeUnexpectedStruct>),
    ParseError(String),
    IOErr(Box<io::Error>),
    UTF8Err(Box<std::str::Utf8Error>),
    BsonErr(Box<BsonErr>),
    DataSizeTooLarge(u32, u32),
    DecodeEOF,
    DataOverflow,
    DataExist(Value),
    PageSpaceNotEnough,
    DataHasNoPrimaryKey,
    ChecksumMismatch,
    JournalPageSizeMismatch(u32, u32),
    SaltMismatch,
    PageMagicMismatch(u32),
    ItemSizeGreaterThanExpected,
    CollectionNotFound(String),
    CollectionIdNotFound(u32),
    MetaPageIdError,
    CannotWriteDbWithoutTransaction,
    StartTransactionInAnotherTransaction,
    RollbackNotInTransaction,
    IllegalCollectionName(String),
    UnexpectedHeaderForBtreePage,
    KeyTypeOfBtreeShouldNotBeZero,
    UnexpectedPageHeader,
    UnexpectedPageType,
    UnknownTransactionType,
    BufferNotEnough(usize),
    UnknownUpdateOperation(String),
    IncrementNullField,
    VmIsHalt,
    MetaVersionMismatched(u32, u32),
    Busy
}

impl fmt::Display for DbErr {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DbErr::UnexpectedIdType(expected_ty, actual_ty) => {
                let expected = ty_int::to_str(*expected_ty);
                let actual = ty_int::to_str(*actual_ty);

                write!(f, "UnexpectedIdType(expected: {}, actual: {})", expected, actual)
            }

            DbErr::NotAValidKeyType(ty_name) => write!(f, "type {} is not a valid key type", ty_name),
            DbErr::NotAValidField(field) => write!(f, "the value of field: \"{}\" is invalid", field),
            DbErr::ValidationError(reason) => write!(f, "ValidationError: {}", reason),
            DbErr::InvalidOrderOfIndex(index_key_name) => write!(f, "invalid order of index: {}", index_key_name),
            DbErr::IndexAlreadyExists(index_key_name) => write!(f, "index for {} already exists", index_key_name),
            DbErr::FieldTypeUnexpected(st) => write!(f, "{}", st),
            DbErr::ParseError(reason) => write!(f, "ParseError: {}", reason),
            DbErr::IOErr(io_err) => io_err.fmt(f),
            DbErr::UTF8Err(utf8_err) => utf8_err.fmt(f),
            DbErr::BsonErr(bson_err) => write!(f, "bson error: {}", bson_err),
            DbErr::DataSizeTooLarge(expected, actual) =>
                write!(f, "DataSizeTooLarge(expected: {}, actual: {})", expected, actual),
            DbErr::DecodeEOF => write!(f, "DecodeEOF"),
            DbErr::DataOverflow => write!(f, "DataOverflow"),
            DbErr::DataExist(value) => write!(f, "item with primary key exists, key: {}", value),
            DbErr::PageSpaceNotEnough => write!(f, "the space of page is not enough"),
            DbErr::DataHasNoPrimaryKey => write!(f, "DataHasNoPrimaryKey"),
            DbErr::ChecksumMismatch => write!(f, "journal's checksum is mismatch with data, database maybe corrupt"),
            DbErr::JournalPageSizeMismatch(expect, actual) => {
                write!(f, "journal's page size is mismatch with database. expect:{}, actual: {}", expect, actual)
            },
            DbErr::SaltMismatch => write!(f, "SaltMismatch"),
            DbErr::PageMagicMismatch(pid) => write!(f, "PageMagicMismatch({})", pid),
            DbErr::ItemSizeGreaterThanExpected => write!(f, "the size of the item is greater than expected"),
            DbErr::CollectionNotFound(name) => write!(f, "collection \"{}\" not found", name),
            DbErr::CollectionIdNotFound(id) => write!(f, "colleciton id {} not found", id),
            DbErr::MetaPageIdError => write!(f, "meta page id should not be zero"),
            DbErr::CannotWriteDbWithoutTransaction => write!(f, "cannot write database without transaction"),
            DbErr::StartTransactionInAnotherTransaction => write!(f, "start transaction in another transaction"),
            DbErr::RollbackNotInTransaction => write!(f, "can not rollback because not in transaction"),
            DbErr::IllegalCollectionName(name) => write!(f, "collection name \"{}\" is illegal", name),
            DbErr::UnexpectedHeaderForBtreePage => write!(f, "unexpected header for btree page"),
            DbErr::KeyTypeOfBtreeShouldNotBeZero => write!(f, "key type of btree should not be zero"),
            DbErr::UnexpectedPageHeader => write!(f, "unexpected page header"),
            DbErr::UnexpectedPageType => write!(f, "unexpected page type"),
            DbErr::UnknownTransactionType => write!(f, "unknown transaction type"),
            DbErr::BufferNotEnough(buffer_size) => write!(f, "buffer not enough, {} needed", buffer_size),
            DbErr::UnknownUpdateOperation(op) => write!(f, "unknown update operation: '{}'", op),
            DbErr::IncrementNullField => write!(f, "can not increment a field which is null"),
            DbErr::VmIsHalt => write!(f, "Vm can not execute because it's halt"),
            DbErr::MetaVersionMismatched(expected, actual) => write!(f, "meta version mismatched, expect: {}, actual: {}", expected, actual),
            DbErr::Busy => write!(f, "database busy"),
        }
    }

}

impl From<BsonErr> for DbErr {

    fn from(error: BsonErr) -> Self {
        DbErr::BsonErr(Box::new(error))
    }

}

impl From<io::Error> for DbErr {

    fn from(error: io::Error) -> Self {
        DbErr::IOErr(Box::new(error))
    }

}

impl From<std::str::Utf8Error> for DbErr {

    fn from(error: std::str::Utf8Error) -> Self {
        DbErr::UTF8Err(Box::new(error))
    }

}

#[cfg(test)]
mod tests {
    use crate::DbErr;

    #[test]
    fn print_value_size() {
        let size = std::mem::size_of::<DbErr>();
        assert_eq!(size, 32);
    }

}
