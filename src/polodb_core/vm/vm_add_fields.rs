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

use bson::Bson;
use indexmap::IndexMap;
use crate::vm::operators::{OpRegistry, OperatorExpr};
use crate::vm::vm_external_func::{VmExternalFunc, VmExternalFuncStatus};
use crate::{Result, Error};
use crate::errors::mk_invalid_aggregate_field;

pub(crate) struct VmFuncAddFields {
    fields: IndexMap<String, OperatorExpr>,
}

impl VmFuncAddFields {

    pub(crate) fn compile(paths: &mut Vec<String>, registry: OpRegistry, value: &Bson) -> Result<Box<dyn VmExternalFunc>> {
        let fields = match value {
            Bson::Document(doc) => {
                let mut fields = IndexMap::new();
                for (k, v) in doc.iter() {
                    let op = crate::path_hint_3!(paths, k.clone(), {
                        match v {
                            Bson::Document(v) => {
                                let op = registry.compile_doc(paths, v)?;
                                OperatorExpr::Expr(op)
                            }
                            Bson::String(field_name) => {
                                if let Some(stripped_field_name) = field_name.strip_prefix("$") {
                                    OperatorExpr::Alias(stripped_field_name.to_string())
                                } else {
                                    OperatorExpr::Constant(Bson::String(field_name.clone()))
                                }
                            }
                            _ => OperatorExpr::Constant(v.clone()),
                        }
                    });
                    fields.insert(k.clone(), op);
                }
                fields
            }
            _ => {
                let invalid_err = mk_invalid_aggregate_field(paths);
                return Err(Error::InvalidField(invalid_err));
            }
        };
        Ok(Box::new(VmFuncAddFields {
            fields,
        }))
    }

}

impl VmExternalFunc for VmFuncAddFields {
    fn name(&self) -> &str {
        "addFields"
    }

    fn call(&self, args: &[Bson]) -> Result<VmExternalFuncStatus> {
        let arg0 = &args[0];
        if arg0.as_null().is_some() {
            return Ok(VmExternalFuncStatus::Next(Bson::Null));
        }
        let mut doc = match arg0 {
            Bson::Document(doc) => doc.clone(),
            _ => return Err(Error::UnknownAggregationOperation("Invalid argument for $addFields".to_string())),
        };
        for (k, v) in &self.fields {
            let value = match v {
                OperatorExpr::Expr(op) => op.next(arg0),
                OperatorExpr::Constant(v) => v.clone(),
                OperatorExpr::Alias(alias) => {
                    let alias = alias.as_str();
                    doc.get(alias).cloned().unwrap_or(Bson::Null)
                }
            };
            doc.insert(k.clone(), value);
        }
        Ok(VmExternalFuncStatus::Next(Bson::Document(doc)))
    }

    fn is_completed(&self) -> bool {
        true
    }
}
