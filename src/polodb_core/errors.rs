// Copyright 2024 Vincent Chan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bson::ser::Error as BsonErr;
use bson::Document;
use std::fmt;
use std::io;
use std::string::FromUtf8Error;
use std::sync::PoisonError;
use thiserror::Error;

#[derive(Debug)]
pub struct FieldTypeUnexpectedStruct {
    pub field_name: String,
    pub expected_ty: String,
    pub actual_ty: String,
}

impl From<FieldTypeUnexpectedStruct> for Error {
    fn from(value: FieldTypeUnexpectedStruct) -> Self {
        Error::FieldTypeUnexpected(Box::new(value))
    }
}

#[derive(Debug)]
pub struct CannotApplyOperationForTypes {
    pub op_name: String,
    pub field_name: String,
    pub field_type: String,
    pub target_type: String,
}

impl From<CannotApplyOperationForTypes> for Error {
    fn from(value: CannotApplyOperationForTypes) -> Self {
        Error::CannotApplyOperation(Box::new(value))
    }
}

impl fmt::Display for FieldTypeUnexpectedStruct {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "unexpected type for field '{}', expected: {}, actual: {}",
            self.field_name, self.expected_ty, self.actual_ty
        )
    }
}

#[derive(Debug)]
pub struct UnexpectedHeader {
    pub page_id: u32,
    pub actual_header: [u8; 2],
    pub expected_header: [u8; 2],
}

impl fmt::Display for UnexpectedHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "page_id: {}, expected header: 0x{:02X} 0x{:02X}, actual: 0x{:02X} 0x{:02X}",
            self.page_id,
            self.expected_header[0],
            self.expected_header[1],
            self.actual_header[0],
            self.actual_header[1]
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

impl From<UnexpectedTypeForOpStruct> for Error {
    fn from(value: UnexpectedTypeForOpStruct) -> Self {
        Error::UnexpectedTypeForOp(Box::new(value))
    }
}

#[derive(Debug)]
pub struct VersionMismatchError {
    pub actual_version: [u8; 4],
    pub expect_version: [u8; 4],
}

#[derive(Debug)]
pub struct BtWrapper<T> {
    pub source: T,
    pub backtrace: std::backtrace::Backtrace,
}

#[derive(Debug)]
pub struct DataMalformedReason {
    pub backtrace: std::backtrace::Backtrace,
}

#[derive(Debug)]
pub struct DuplicateKeyError {
    pub name: String, // index name
    pub key: String,  // key name
    pub ns: String,   // collection name
}

#[derive(Debug)]
pub struct RegexError {
    pub error: String,
    pub expression: String,
    pub options: String,
}

#[derive(Error, Debug)]
pub enum Error {
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
    #[error("io error: {}, backtrace: {}", .0.source, .0.backtrace)]
    IOErr(Box<BtWrapper<io::Error>>),
    #[error("utf8 error: {source}")]
    UTF8Err {
        #[from]
        source: std::str::Utf8Error,
    },
    #[error("bson error: {}, , backtrace: {}", .0.source, .0.backtrace)]
    BsonErr(Box<BtWrapper<BsonErr>>),
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
    #[error("index name '{0}' is illegal")]
    IllegalIndexName(String),
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
    Multiple(Vec<Error>),
    #[error("db version mismatched, please upgrade")]
    VersionMismatch(Box<VersionMismatchError>),
    #[error("the mutex is poisoned")]
    LockError,
    #[error("can not operation {} for '{}' with types {} and {}", .0.op_name, .0.field_name, .0.field_type, .0.target_type)]
    CannotApplyOperation(Box<CannotApplyOperationForTypes>),
    #[error("no transaction started")]
    NoTransactionStarted,
    #[error("session is outdated")]
    SessionOutdated,
    #[error("the database is closed")]
    DbIsClosed,
    #[error("{0}")]
    FromUtf8Error(Box<FromUtf8Error>),
    #[error("the database is not ready")]
    DbNotReady,
    #[error("only support single field indexes currently: {0:?}")]
    OnlySupportSingleFieldIndexes(Box<Document>),
    #[error("only support ascending order index currently: {0}")]
    OnlySupportsAscendingOrder(String),
    #[error("duplicate key error collection: {}, index: {}, key: {}", .0.ns, .0.name, .0.key)]
    DuplicateKey(Box<DuplicateKeyError>),
    #[error("the element type {0} is unknown")]
    UnknownBsonElementType(u8),
    #[error("failed to run regex expression: {}, expression: {}, options: {}", .0.error, .0.expression, .0.options)]
    RegexError(Box<RegexError>),
    #[error("unknown aggression operation: {0}")]
    UnknownAggregationOperation(String),
    #[error("invalid aggregation stage: {0:?}")]
    InvalidAggregationStage(Box<Document>),
    #[error("rocks db error: {0}")]
    RocksDbErr(String),
}

impl Error {
    pub(crate) fn add(self, next: Error) -> Error {
        match self {
            Error::Multiple(mut result) => {
                result.push(next);
                Error::Multiple(result)
            }
            _ => {
                let result = vec![self, next];
                Error::Multiple(result)
            }
        }
    }
}

impl From<bson::de::Error> for Error {
    fn from(error: bson::de::Error) -> Self {
        Error::BsonDeErr(Box::new(error))
    }
}

impl From<BsonErr> for Error {
    fn from(error: BsonErr) -> Self {
        Error::BsonErr(Box::new(BtWrapper {
            source: error,
            backtrace: std::backtrace::Backtrace::capture(),
        }))
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Error::LockError
    }
}

impl From<FromUtf8Error> for Error {
    fn from(value: FromUtf8Error) -> Self {
        Error::FromUtf8Error(Box::new(value))
    }
}

impl From<DuplicateKeyError> for Error {
    fn from(value: DuplicateKeyError) -> Self {
        Error::DuplicateKey(Box::new(value))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::IOErr(Box::new(BtWrapper {
            source: value,
            backtrace: std::backtrace::Backtrace::capture(),
        }))
    }
}

impl From<RegexError> for Error {
    fn from(value: RegexError) -> Self {
        Error::RegexError(Box::new(value))
    }
}

#[cfg(test)]
mod tests {
    use crate::Error;

    #[test]
    fn print_value_size() {
        let size = std::mem::size_of::<Error>();
        assert_eq!(size, 32);
    }
}
