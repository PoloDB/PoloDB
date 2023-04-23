/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::fmt;
use std::sync::PoisonError;
use error_chain::error_chain;

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

#[derive(Debug)]
pub struct UnexpectedHeader {
    pub page_id: u32,
    pub actual_header: [u8; 2],
    pub expected_header: [u8; 2],
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

#[derive(Debug)]
pub struct UnexpectedTypeForOpStruct {
    pub operation: &'static str,
    pub expected_ty: &'static str,
    pub actual_ty: String,
}

#[derive(Debug)]
pub struct VersionMismatchError {
    pub actual_version: [u8; 4],
    pub expect_version: [u8; 4],
}

error_chain! {

    types {
        Error, ErrorKind, ResultExt, Result;
    }

    foreign_links {
        Fmt(::std::fmt::Error);
        Io(::std::io::Error) #[cfg(unix)];
        Utf8(::std::str::Utf8Error);
        FromUtf8(::std::string::FromUtf8Error);
        Bson(bson::ser::Error);
        BsonDe(bson::de::Error);
    }

    errors {

        CollectionNotFound(name: String) {
            description("collection not found")
            display("collection not found: '{}'", name)
        }

        IllegalCollectionName(name: String) {
            description("illegal collection name")
            display("illegal collection name: '{}'", name)
        }

        CollectionAlreadyExits(name: String) {
            description("collection already exists")
            display("collection already exists: '{}'", name)
        }

        FieldTypeUnexpected(v: Box<FieldTypeUnexpectedStruct>) {
            description("field type unexpected")
            display("{}", v)
        }

        InvalidField(st: Box<InvalidFieldStruct>) {
            description("invalid field")
            display("the {} field name: \"{}\" is invalid, path: {}",
               st.field_type,
               st.field_name,
               st.path
                   .as_ref()
                   .unwrap_or(&String::from("<None>"))
            )
        }

        UnknownUpdateOperation(op: String) {
            description("unknown update operation")
            display("unknown update operation: '{}'", op)
        }

        UnableToUpdatePrimaryKey {
            description("unable to update primary key")
            display("it's illegal to update '_id' field")
        }

        IncrementNullField {
            description("increment null field")
            display("can not increment a field which is null")
        }

        CannotApplyOperation(msg: Box<CannotApplyOperationForTypes>) {
            description("can not apply operation")
            display("can not operation {} for \"{}\" with types {} and {}",
                msg.op_name,
                msg.field_name,
                msg.field_type,
                msg.target_type
            )
        }

        UnexpectedTypeForOp(st: Box<UnexpectedTypeForOpStruct>) {
            description("unexpected type for op")
            display("unexpected type: {} for op: {}, expected: {}",
                st.actual_ty,
                st.operation,
                st.expected_ty
            )
        }

        VmIsHalt {
            description("vm is halt")
            display("Vm can not execute because it's halt")
        }

        StartTransactionInAnotherTransaction {
            description("start transaction in another transaction")
            display( "start transaction in another transaction")
        }

        NoTransactionStarted {
            description("no transaction started")
            display("no transaction started")
        }

        DbIsClosed {
            description("db is closed")
        }

        LockError {
            description("lock error")
        }

        DatabaseOccupied {
            description("data occupied")
        }

        NotAValidDatabase {
            description("not a valid database")
        }

        DataMalformed {
            description("data malformed")
        }

        DbNotReady {
            description("database is not ready")
        }

        ChecksumMismatch {
            description("checksum mismatch")
        }

        SessionOutdated {
            description("session is outdated")
        }

        NotAValidKeyType(name: String) {
            description("not a valid key type")
            display("type {} is not a valid key type", name),
        }

    }

}

impl From<FieldTypeUnexpectedStruct> for Error {
    fn from(value: FieldTypeUnexpectedStruct) -> Self {
        ErrorKind::FieldTypeUnexpected(Box::new(value)).into()
    }
}

impl From<InvalidFieldStruct> for Error {
    fn from(value: InvalidFieldStruct) -> Self {
        ErrorKind::InvalidField(Box::new(value)).into()
    }
}

impl From<CannotApplyOperationForTypes> for Error {
    fn from(value: CannotApplyOperationForTypes) -> Self {
        ErrorKind::CannotApplyOperation(Box::new(value)).into()
    }
}

impl From<UnexpectedTypeForOpStruct> for Error {
    fn from(value: UnexpectedTypeForOpStruct) -> Self {
        ErrorKind::UnexpectedTypeForOp(Box::new(value)).into()
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        ErrorKind::LockError.into()
    }
}
