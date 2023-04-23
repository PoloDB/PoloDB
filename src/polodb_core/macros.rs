/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[macro_export]
macro_rules! polo_log (
    ($($arg:tt)+) => {
        if crate::db::SHOULD_LOG.load(std::sync::atomic::Ordering::SeqCst) {
            eprintln!($($arg)*);
        }
    }
);

#[macro_export]
macro_rules! try_unwrap_document {
    ($op_name:tt, $doc:expr) => {
        match $doc {
            Bson::Document(doc) => doc,
            t => {
                let name = format!("{}", t);
                return Err(crate::errors::FieldTypeUnexpectedStruct {
                    field_name: $op_name.into(),
                    expected_ty: "Document".into(),
                    actual_ty: name,
                }.into());
            },
        }
    };
}

#[macro_export]
macro_rules! try_unwrap_array {
    ($op_name:tt, $arr:expr) => {
        match $arr {
            Bson::Array(arr) => arr,
            t => {
                let name = format!("{}", t);
                return Err(crate::errors::FieldTypeUnexpectedStruct {
                    field_name: $op_name.into(),
                    expected_ty: "Array".into(),
                    actual_ty: name,
                }.into());
            },
        }
    };
}

