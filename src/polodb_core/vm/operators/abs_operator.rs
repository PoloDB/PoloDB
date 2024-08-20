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
use crate::vm::operators::{OpRegistry, OperatorExpr, VmOperator};
use crate::{Result, Error};

pub(crate) struct AbsOperator {
    inner: OperatorExpr,
}

impl AbsOperator {

    fn bson_abs(input: Bson) -> Bson {
        match input {
            Bson::Int32(v) => {
                let result = v.abs();
                Bson::Int32(result)
            }
            Bson::Int64(v) => {
                let result = v.abs();
                Bson::Int64(result)
            }
            _ => input,
        }
    }

    pub(crate) fn compile(registry: OpRegistry, v: &Bson) -> Result<Box<dyn VmOperator>> {
        let inner = match v {
            Bson::Document(doc) => {
                let op = registry.compile_doc(doc)?;
                OperatorExpr::Expr(op)
            }
            Bson::Null => {
                OperatorExpr::Constant(Bson::Null)
            }
            Bson::Int32(v) => {
               let result = v.abs();
                OperatorExpr::Constant(Bson::Int32(result))
            }
            Bson::Int64(v) => {
                let result = v.abs();
                OperatorExpr::Constant(Bson::Int64(result))
            }
            Bson::String(field_name) => {
                if field_name.starts_with("$") {
                    let field_name = field_name[1..].to_string();
                    OperatorExpr::Alias(field_name)
                } else {
                    return Err(Error::UnknownAggregationOperation("$abs".to_string()));
                }
            }
            _ => {
                return Err(Error::UnknownAggregationOperation("$abs".to_string()));
            }
        };
        Ok(Box::new(AbsOperator {
            inner,
        }))
    }

}

impl VmOperator for AbsOperator {
    fn initial_value(&self) -> Bson {
        match self.inner {
            OperatorExpr::Constant(ref v) => v.clone(),
            OperatorExpr::Expr(ref op) =>
                Self::bson_abs(op.initial_value()),
            OperatorExpr::Alias(_) => Bson::Null,
        }
    }

    fn next(&self, input: &Bson) -> Bson {
        match self.inner {
            OperatorExpr::Constant(ref v) => v.clone(),
            OperatorExpr::Expr(ref op) =>
                Self::bson_abs(op.next(input)),
            OperatorExpr::Alias(ref field_name) => {
                let unwrap = match input {
                    Bson::Document(doc) => doc.get(field_name).cloned(),
                    _ => None,
                }.unwrap_or(Bson::Null);
                Self::bson_abs(unwrap)
            }
        }
    }

    fn complete(&self) -> Bson {
        match self.inner {
            OperatorExpr::Constant(ref v) => v.clone(),
            OperatorExpr::Expr(ref op) =>
                Self::bson_abs(op.complete()),
            OperatorExpr::Alias(_) => Bson::Null,
        }
    }
}
