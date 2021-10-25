use std::io;
use std::fmt;
use std::rc::Rc;
use polodb_bson::{Value, ty_int};
use polodb_bson::error::BsonErr;

#[derive(Debug)]
pub struct FieldTypeUnexpectedStruct {
    pub field_name: Box<str>,
    pub expected_ty: &'static str,
    pub actual_ty: &'static str,
}

impl fmt::Display for FieldTypeUnexpectedStruct {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "unexpected type for field '{}', expected: {}, actual: {}",
               self.field_name, self.expected_ty, self.actual_ty)
    }

}

pub(crate) fn mk_field_name_type_unexpected(
    option_name: &str, expected_ty: &'static str, actual_ty: &'static str
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
    pub actual_ty: &'static str,
}

pub fn mk_unexpected_type_for_op(op: &'static str, expected_ty: &'static str, actual_ty: &'static str) -> Box<UnexpectedTypeForOpStruct> {
    Box::new(UnexpectedTypeForOpStruct {
        operation: op,
        expected_ty,
        actual_ty
    })
}

#[derive(Debug)]
pub enum DbErr {
    UnexpectedIdType(u8, u8),
    NotAValidKeyType(String),
    InvalidField(Box<InvalidFieldStruct>),
    ValidationError(String),
    InvalidOrderOfIndex(String),
    IndexAlreadyExists(String),
    FieldTypeUnexpected(Box<FieldTypeUnexpectedStruct>),
    UnexpectedTypeForOp(Box<UnexpectedTypeForOpStruct>),
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
    UnexpectedHeaderForBtreePage(Box<UnexpectedHeader>),
    KeyTypeOfBtreeShouldNotBeZero,
    UnexpectedPageHeader,
    UnexpectedPageType,
    UnknownTransactionType,
    BufferNotEnough(usize),
    UnknownUpdateOperation(Rc<str>),
    IncrementNullField,
    VmIsHalt,
    MetaVersionMismatched(u32, u32),
    CollectionAlreadyExits(String),
    UnableToUpdatePrimaryKey,
    NotAValidDatabase,
    Busy,
    DatabaseOccupied,
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
            DbErr::InvalidField(st) =>
                write!(f, "the {} field name: \"{}\" is invalid, path: {}",
                       st.field_type, st.field_name, st.path.as_ref().unwrap_or(&String::from("<None>"))),
            DbErr::ValidationError(reason) => write!(f, "ValidationError: {}", reason),
            DbErr::InvalidOrderOfIndex(index_key_name) => write!(f, "invalid order of index: {}", index_key_name),
            DbErr::IndexAlreadyExists(index_key_name) => write!(f, "index for {} already exists", index_key_name),
            DbErr::FieldTypeUnexpected(st) => write!(f, "{}", st),
            DbErr::UnexpectedTypeForOp(st) =>
                write!(f, "unexpected type: {} for op: {}, expected: {}", st.actual_ty, st.operation, st.expected_ty),
            DbErr::ParseError(reason) => write!(f, "ParseError: {}", reason),
            DbErr::IOErr(io_err) => write!(f, "IOErr: {}", io_err),
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
            DbErr::UnexpectedHeaderForBtreePage(err) => write!(f, "unexpected header for btree page: {}", err),
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
            DbErr::CollectionAlreadyExits(name) => write!(f, "collection name '{}' already exists", name),
            DbErr::UnableToUpdatePrimaryKey => write!(f, "it's illegal to update '_id' field"),
            DbErr::NotAValidDatabase => write!(f, "the file is not a valid database"),
            DbErr::DatabaseOccupied => write!(f, "this file is occupied by another connection"),
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
