/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::io;
use std::fmt;
use std::string::FromUtf8Error;
use std::sync::PoisonError;
use bson::oid::ObjectId;
use bson::ser::Error as BsonErr;
use thiserror::Error;

#[derive(Debug)]
pub struct FieldTypeUnexpectedStruct {
    pub field_name: String,
    pub expected_ty: String,
    pub actual_ty: String,
}

#[derive(Debug)]
pub struct CannotApplyOperationForTypes {
    pub op_name: String,
    pub field_name: String,
    pub field_type: String,
    pub target_type: String,
}

impl fmt::Display for FieldTypeUnexpectedStruct {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "unexpected type for field '{}', expected: {}, actual: {}",
               self.field_name, self.expected_ty, self.actual_ty)
    }

}

pub(crate) fn mk_field_name_type_unexpected(
    option_name: String, expected_ty: String, actual_ty: String
) -> DbErr {
    DbErr::FieldTypeUnexpected(Box::new(FieldTypeUnexpectedStruct {
        field_name: option_name.into(),
        expected_ty,
        actual_ty,
    }))
}

#[derive(Debug)]
pub struct UnexpectedHeader {
    pub page_id: u32,
    pub actual_header: [u8; 2],
    pub expected_header: [u8; 2],
}

#[allow(dead_code)]
pub(crate) fn mk_unexpected_header_for_btree_page(page_id: u32, actual: &[u8], expected: &[u8]) -> DbErr {
    let mut actual_header: [u8; 2] = [0; 2];
    let mut expected_header: [u8; 2] = [0; 2];
    actual_header.copy_from_slice(actual);
    expected_header.copy_from_slice(expected);
    DbErr::UnexpectedHeaderForBtreePage(Box::new(UnexpectedHeader {
        page_id,
        actual_header,
        expected_header,
    }))
}

impl fmt::Display for UnexpectedHeader {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "page_id: {}, expected header: 0x{:02X} 0x{:02X}, actual: 0x{:02X} 0x{:02X}",
               self.page_id,
            self.expected_header[0], self.expected_header[1],
            self.actual_header[0], self.actual_header[1]
        )
    }

}

#[derive(Debug)]
pub struct InvalidFieldStruct {
    pub field_type: &'static str,
    pub field_name: String,
    pub path: Option<String>,
}

pub fn mk_invalid_query_field(name: String, path: String) -> Box<InvalidFieldStruct> {
    Box::new(InvalidFieldStruct {
        field_type: "query",
        field_name: name,
        path: Some(path),
    })
}

#[derive(Debug)]
pub struct UnexpectedTypeForOpStruct {
    pub operation: &'static str,
    pub expected_ty: &'static str,
    pub actual_ty: String,
}

pub fn mk_unexpected_type_for_op(op: &'static str, expected_ty: &'static str, actual_ty: String) -> Box<UnexpectedTypeForOpStruct> {
    Box::new(UnexpectedTypeForOpStruct {
        operation: op,
        expected_ty,
        actual_ty
    })
}

#[derive(Debug)]
pub struct VersionMismatchError {
    pub actual_version: [u8; 4],
    pub expect_version: [u8; 4],
}

#[derive(Error, Debug)]
pub enum DbErr {
    #[error("unexpected id type, expected: {0}, actual: {1}")]
    UnexpectedIdType(u8, u8),
    #[error("type '{0}' is not a valid key type")]
    NotAValidKeyType(String),
    #[error("the {} field name: '{}' is invalid, path: {:?}", .0.field_type, .0.field_name, .0.path)]
    InvalidField(Box<InvalidFieldStruct>),
    #[error("validation error: {0}")]
    ValidationError(String),
    #[error("invalid order of index: {0}")]
    InvalidOrderOfIndex(String),
    #[error("index for '{0}' already exists")]
    IndexAlreadyExists(String),
    #[error("{0}")]
    FieldTypeUnexpected(Box<FieldTypeUnexpectedStruct>),
    #[error("unexpected type: {} for op: {}, expected: {}", .0.actual_ty, .0.operation, .0.expected_ty)]
    UnexpectedTypeForOp(Box<UnexpectedTypeForOpStruct>),
    #[error("parse error: {0}")]
    ParseError(String),
    #[error("io error: {0}")]
    IOErr(Box<io::Error>),
    #[error("utf8 error: {0}")]
    UTF8Err(Box<std::str::Utf8Error>),
    #[error("bson error: {0}")]
    BsonErr(Box<BsonErr>),
    #[error("bson de error: {0}")]
    BsonDeErr(Box<bson::de::Error>),
    #[error("data size too large, expected: {0}, actual: {1}")]
    DataSizeTooLarge(u32, u32),
    #[error("decode EOF")]
    DecodeEOF,
    #[error("data overflow")]
    DataOverflow,
    #[error("item with primary key exists, key: '{0}'")]
    DataExist(String),
    #[error("the space of page is not enough")]
    PageSpaceNotEnough,
    #[error("data has no primary key")]
    DataHasNoPrimaryKey,
    #[error("journal's checksum is mismatch with data, database maybe corrupt")]
    ChecksumMismatch,
    #[error("journal's page size is mismatch with database. expect: {0}, actual: {1}")]
    JournalPageSizeMismatch(u32, u32),
    #[error("salt mismatch")]
    SaltMismatch,
    #[error("page magic mismatch: {0}")]
    PageMagicMismatch(u32),
    #[error("the size of the item is greater than expected")]
    ItemSizeGreaterThanExpected,
    #[error("collection '{0}' not found")]
    CollectionNotFound(String),
    #[error("meta page id should not be zero")]
    MetaPageIdError,
    #[error("cannot write database without transaction")]
    CannotWriteDbWithoutTransaction,
    #[error("start transaction in another transaction")]
    StartTransactionInAnotherTransaction,
    #[error("can not rollback because not in transaction")]
    RollbackNotInTransaction,
    #[error("collection name '{0}' is illegal")]
    IllegalCollectionName(String),
    #[error("unexpected header for btree page: {0}")]
    UnexpectedHeaderForBtreePage(Box<UnexpectedHeader>),
    #[error("key type of btree should not be zero")]
    KeyTypeOfBtreeShouldNotBeZero,
    #[error("unexpected page header")]
    UnexpectedPageHeader,
    #[error("unexpected page type")]
    UnexpectedPageType,
    #[error("unknown transaction type")]
    UnknownTransactionType,
    #[error("buffer not enough, {0} needed")]
    BufferNotEnough(usize),
    #[error("unknown update operation: '{0}'")]
    UnknownUpdateOperation(String),
    #[error("can not increment a field which is null")]
    IncrementNullField,
    #[error("VM can not execute because it's halt")]
    VmIsHalt,
    #[error("collection name '{0}' already exists")]
    CollectionAlreadyExits(String),
    #[error("it's illegal to update '_id' field")]
    UnableToUpdatePrimaryKey,
    #[error("the file is not a valid database")]
    NotAValidDatabase,
    #[error("database busy")]
    Busy,
    #[error("this file is occupied by another connection")]
    DatabaseOccupied,
    #[error("multiple errors")]
    Multiple(Vec<DbErr>),
    #[error("db version mismatched, please upgrade")]
    VersionMismatch(Box<VersionMismatchError>),
    #[error("the mutex is poisoned")]
    LockError,
    #[error("can not operation {} for '{}' with types {} and {}", .0.op_name, .0.field_name, .0.field_type, .0.target_type)]
    CannotApplyOperation(Box<CannotApplyOperationForTypes>),
    #[error("no transaction started")]
    NoTransactionStarted,
    #[error("invalid session: {0}")]
    InvalidSession(Box<ObjectId>),
    #[error("session is outdated")]
    SessionOutdated,
    #[error("the database is closed")]
    DbIsClosed,
    #[error("{0}")]
    FromUtf8Error(Box<FromUtf8Error>),
    #[error("data malformed")]
    DataMalformed,
    #[error("the database is not ready")]
    DbNotReady,
}

impl DbErr {

    pub(crate) fn add(self, next: DbErr) -> DbErr {
        match self {
            DbErr::Multiple(mut result) => {
                result.push(next);
                DbErr::Multiple(result)
            }
            _ => {
                let result = vec![self, next];
                DbErr::Multiple(result)
            }
        }
    }

}
//
// impl fmt::Display for DbErr {
//
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         match self {
//             DbErr::UnexpectedIdType(expected_ty, actual_ty) => {
//                 write!(f, "UnexpectedIdType(expected: {}, actual: {})", expected_ty, actual_ty)
//             }
//
//             DbErr::NotAValidKeyType(ty_name) => write!(f, "type {} is not a valid key type", ty_name),
//             DbErr::InvalidField(st) =>
//                 write!(f, "the {} field name: \"{}\" is invalid, path: {}",
//                        st.field_type, st.field_name, st.path.as_ref().unwrap_or(&String::from("<None>"))),
//             DbErr::ValidationError(reason) => write!(f, "ValidationError: {}", reason),
//             DbErr::InvalidOrderOfIndex(index_key_name) => write!(f, "invalid order of index: {}", index_key_name),
//             DbErr::IndexAlreadyExists(index_key_name) => write!(f, "index for {} already exists", index_key_name),
//             DbErr::FieldTypeUnexpected(st) => write!(f, "{}", st),
//             DbErr::UnexpectedTypeForOp(st) =>
//                 write!(f, "unexpected type: {} for op: {}, expected: {}", st.actual_ty, st.operation, st.expected_ty),
//             DbErr::ParseError(reason) => write!(f, "ParseError: {}", reason),
//             DbErr::IOErr(io_err) => write!(f, "IOErr: {}", io_err),
//             DbErr::UTF8Err(utf8_err) => utf8_err.fmt(f),
//             DbErr::BsonErr(bson_err) => write!(f, "bson error: {}", bson_err),
//             DbErr::BsonDeErr(bson_de_err) => write!(f, "bson de error: {}", bson_de_err),
//             DbErr::DataSizeTooLarge(expected, actual) =>
//                 write!(f, "DataSizeTooLarge(expected: {}, actual: {})", expected, actual),
//             DbErr::DecodeEOF => write!(f, "DecodeEOF"),
//             DbErr::DataOverflow => write!(f, "DataOverflow"),
//             DbErr::DataExist(value) => write!(f, "item with primary key exists, key: {}", value),
//             DbErr::PageSpaceNotEnough => write!(f, "the space of page is not enough"),
//             DbErr::DataHasNoPrimaryKey => write!(f, "DataHasNoPrimaryKey"),
//             DbErr::ChecksumMismatch => write!(f, "journal's checksum is mismatch with data, database maybe corrupt"),
//             DbErr::JournalPageSizeMismatch(expect, actual) => {
//                 write!(f, "journal's page size is mismatch with database. expect:{}, actual: {}", expect, actual)
//             },
//             DbErr::SaltMismatch => write!(f, "SaltMismatch"),
//             DbErr::PageMagicMismatch(pid) => write!(f, "PageMagicMismatch({})", pid),
//             DbErr::ItemSizeGreaterThanExpected => write!(f, "the size of the item is greater than expected"),
//             DbErr::CollectionNotFound(name) => write!(f, "collection \"{}\" not found", name),
//             DbErr::MetaPageIdError => write!(f, "meta page id should not be zero"),
//             DbErr::CannotWriteDbWithoutTransaction => write!(f, "cannot write database without transaction"),
//             DbErr::StartTransactionInAnotherTransaction => write!(f, "start transaction in another transaction"),
//             DbErr::RollbackNotInTransaction => write!(f, "can not rollback because not in transaction"),
//             DbErr::IllegalCollectionName(name) => write!(f, "collection name \"{}\" is illegal", name),
//             DbErr::UnexpectedHeaderForBtreePage(err) => write!(f, "unexpected header for btree page: {}", err),
//             DbErr::KeyTypeOfBtreeShouldNotBeZero => write!(f, "key type of btree should not be zero"),
//             DbErr::UnexpectedPageHeader => write!(f, "unexpected page header"),
//             DbErr::UnexpectedPageType => write!(f, "unexpected page type"),
//             DbErr::UnknownTransactionType => write!(f, "unknown transaction type"),
//             DbErr::BufferNotEnough(buffer_size) => write!(f, "buffer not enough, {} needed", buffer_size),
//             DbErr::UnknownUpdateOperation(op) => write!(f, "unknown update operation: '{}'", op),
//             DbErr::IncrementNullField => write!(f, "can not increment a field which is null"),
//             DbErr::VmIsHalt => write!(f, "Vm can not execute because it's halt"),
//             DbErr::Busy => write!(f, "database busy"),
//             DbErr::CollectionAlreadyExits(name) => write!(f, "collection name '{}' already exists", name),
//             DbErr::UnableToUpdatePrimaryKey => write!(f, "it's illegal to update '_id' field"),
//             DbErr::NotAValidDatabase => write!(f, "the file is not a valid database"),
//             DbErr::DatabaseOccupied => write!(f, "this file is occupied by another connection"),
//             DbErr::Multiple(errors) => {
//                 for (i, err) in errors.iter().enumerate() {
//                     writeln!(f, "Multiple errors:")?;
//                     writeln!(f, "{}: {}", i, err)?;
//                 }
//                 Ok(())
//             }
//             DbErr::VersionMismatch(err) => {
//                 writeln!(f, "db version mismatched, please upgrade")?;
//                 let actual = err.actual_version;
//                 let expect = err.expect_version;
//                 writeln!(f, "expect: {}.{}.{}.{}", expect[0], expect[1], expect[2], expect[3])?;
//                 writeln!(f, "actual: {}.{}.{}.{}", actual[0], actual[1], actual[2], actual[3])
//             }
//             DbErr::LockError => writeln!(f, "the mutex is poisoned"),
//             DbErr::CannotApplyOperation(msg) =>
//                 write!(f, "can not operation {} for \"{}\" with types {} and {}",
//                        msg.op_name, msg.field_name, msg.field_type, msg.target_type),
//             DbErr::NoTransactionStarted => write!(f, "no transaction started"),
//             DbErr::InvalidSession(sid) => write!(f, "invalid session: {}", sid),
//             DbErr::SessionOutdated => write!(f, "session is outdated"),
//             DbErr::DbIsClosed => write!(f, "the database is closed"),
//             DbErr::FromUtf8Error(err) => write!(f, "{}", err),
//             DbErr::DataMalformed => write!(f, "data malformed"),
//             DbErr::DbNotReady => write!(f, "the database is not ready"),
//         }
//     }
//
// }

impl From<bson::de::Error> for DbErr {

    fn from(error: bson::de::Error) -> Self {
        DbErr::BsonDeErr(Box::new(error))
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

impl<T> From<PoisonError<T>> for DbErr {
    fn from(_: PoisonError<T>) -> Self {
        DbErr::LockError
    }
}

impl From<FromUtf8Error> for DbErr {

    fn from(value: FromUtf8Error) -> Self {
        DbErr::FromUtf8Error(Box::new(value))
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
