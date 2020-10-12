/*
 * Copyright (c) 2020 Vincent Chan
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation; either version 3, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */
use std::io;
use std::fmt;
use polodb_bson::{Value, ty_int};
use polodb_bson::error::BsonErr;

pub mod validation_error_reason {

    pub static ILLEGAL_INDEX_OPTIONS_KEY: &str = "illegal key for index options";
    pub static TYPE_OF_INDEX_NAME_SHOULD_BE_STRING: &str = "type of index name should be string";
    pub static ORDER_OF_INDEX_CAN_ONLY_BE_ONE: &str = "order of index can only be one";
    pub static UNIQUE_PROP_SHOULD_BE_BOOLEAN: &str = "unique prop should be boolean";

}

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
pub(crate) fn mk_index_options_type_unexpected(option_name: &str, expected_ty: &str, actual_ty: &str) -> DbErr {
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
            DbErr::DataExist(value) => write!(f, "DataExist(pkey = {})", value),
            DbErr::PageSpaceNotEnough => write!(f, "the space of page is not enough"),
            DbErr::DataHasNoPrimaryKey => write!(f, "DataHasNoPrimaryKey"),
            DbErr::ChecksumMismatch => write!(f, "ChecksumMismatch"),
            DbErr::JournalPageSizeMismatch(expect, actual) => write!(f, "JournalPageSizeMismatch(expect={}, actual={})", expect, actual),
            DbErr::SaltMismatch => write!(f, "SaltMismatch"),
            DbErr::PageMagicMismatch(pid) => write!(f, "PageMagicMismatch({})", pid),
            DbErr::ItemSizeGreaterThanExpected => write!(f, "the size of the item is greater than expected"),
            DbErr::CollectionNotFound(name) => write!(f, "collection \"{}\" not found", name),
            DbErr::MetaPageIdError => write!(f, "meta page id should not be zero"),
            DbErr::CannotWriteDbWithoutTransaction => write!(f, "cannot write database without transaction"),
            DbErr::StartTransactionInAnotherTransaction => write!(f, "start transaction in another transaction"),
            DbErr::RollbackNotInTransaction => write!(f, "can not rollback because not int transaction"),
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
