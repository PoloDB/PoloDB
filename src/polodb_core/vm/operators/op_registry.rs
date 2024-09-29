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

use bson::{Bson, Document};
use crate::{Error, Result};
use crate::errors::mk_invalid_aggregate_field;
use crate::vm::operators::{AbsOperator, SumOperator, VmOperator};

// Reference: https://www.mongodb.com/docs/manual/reference/operator/aggregation/
#[derive(Clone)]
pub(crate) struct OpRegistry;

impl OpRegistry {

    pub(crate) fn compile(&self, paths: &mut Vec<String>, v: &Bson) -> Result<Box<dyn VmOperator>> {
        if let Bson::Document(doc) = v {
            self.compile_doc(paths, doc)
        } else {
            let invalid_err = mk_invalid_aggregate_field(paths);
            Err(Error::InvalidField(invalid_err))
        }
    }

    pub(crate) fn compile_doc(&self, paths: &mut Vec<String>, doc: &Document) -> Result<Box<dyn VmOperator>> {
        if doc.len() != 1 {
            return Err(Error::ValidationError("Operator should have exactly one field".to_string()));
        }
        let (op_name, op_value) = doc.iter().next().ok_or(Error::ValidationError("Operator should have exactly one field".to_string()))?;
        let op = crate::path_hint_3!(paths, op_name.clone(), {
            match op_name.as_str() {
                "$sum" => SumOperator::compile(op_value),
                "$abs" => AbsOperator::compile(paths, self.clone(), op_value)?,
                _ => {
                    let invalid_err = mk_invalid_aggregate_field(paths);
                    return Err(Error::InvalidField(invalid_err))
                }
            }
        });
        Ok(op)
    }

}
