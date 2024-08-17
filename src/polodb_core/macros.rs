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
                let err = crate::errors::FieldTypeUnexpectedStruct {
                    field_name: $op_name.into(),
                    expected_ty: "Document".into(),
                    actual_ty: name,
                }.into();
                return Err(err);
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
                let err = crate::errors::FieldTypeUnexpectedStruct {
                    field_name: $op_name.into(),
                    expected_ty: "Array".into(),
                    actual_ty: name,
                }.into();
                return Err(err);
            },
        }
    };
}

