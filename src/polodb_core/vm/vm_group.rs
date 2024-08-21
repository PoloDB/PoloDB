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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use bson::Bson;
use crate::vm::vm_external_func::{VmExternalFunc, VmExternalFuncStatus};
use crate::{Result, Error};
use indexmap::IndexMap;
use crate::vm::operators::{OpRegistry, VmOperator};

const NAME: &'static str = "group";

// Reference: https://www.mongodb.com/docs/manual/reference/operator/aggregation/group/
pub(crate) struct VmFuncGroup {
    is_completed: AtomicBool,
    inner: Mutex<VmFuncGroupInner>,
}

struct VmFuncGroupInner {
    group_values: IndexMap<String, Bson>,
    operators: HashMap<String, Box<dyn VmOperator>>,
}

impl VmFuncGroup {

    fn compile_command(
        paths: &mut Vec<String>,
        registry: OpRegistry,
        key: &str,
        value: &Bson,
        group_values: &mut IndexMap<String, Bson>,
        operators: &mut HashMap<String, Box<dyn VmOperator>>,
    ) -> Result<()> {
        let op = registry.compile(paths, value)?;
        group_values.insert(key.into(), op.initial_value());
        operators.insert(key.into(), op);
        Ok(())
    }

    pub(crate) fn compile(
        paths: &mut Vec<String>,
        registry: OpRegistry,
        value: &Bson,
    ) -> Result<Box<dyn VmExternalFunc>> {
        let doc = crate::try_unwrap_document!("$group", value);
        let mut group_values = IndexMap::new();
        let mut operators = HashMap::new();

        let mut found_id = false;
        for (k, v) in doc.iter() {
            crate::path_hint_2!(paths, k.clone(), {
                group_values.insert(k.clone(), v.clone());
                let k_str = k.as_str();
                if k_str == "_id" {
                    found_id = true;
                    paths.pop();
                    continue;
                }

                VmFuncGroup::compile_command(
                    paths,
                    registry.clone(),
                    k_str,
                    v,
                    &mut group_values,
                    &mut operators,
                )?;
            });
        }
        if !found_id {
            let err_msg = "Field '_id' is required for $group".to_string();
            return Err(Error::ValidationError(err_msg));
        }

        let result = VmFuncGroup {
            is_completed: AtomicBool::new(false),
            inner: Mutex::new(VmFuncGroupInner {
                group_values,
                operators,
            }),
        };
        Ok(Box::new(result))
    }
}

impl VmExternalFunc for VmFuncGroup {
    fn name(&self) -> &str {
        NAME
    }

    fn call(&self, args: &[Bson]) -> Result<VmExternalFuncStatus> {
        let arg0 = &args[0];
        let mut inner = self.inner.lock().unwrap();
        if arg0.as_null().is_some() {  // complete
            self.is_completed.store(true, Ordering::Relaxed);
            let mut result = bson::Document::new();
            for (k, value) in inner.group_values.iter() {
                if k == "_id" {
                    result.insert(k.clone(), value.clone());
                    continue;
                }
                let op = inner.operators.get(k).expect("Operator not found");
                let final_value = op.complete();
                result.insert(k.clone(), final_value);
            }
            return Ok(VmExternalFuncStatus::Next(result.into()));
        }

        let next_map = inner.group_values
            .iter()
            .map(|(k, v)| {
                if k == "_id" {
                    return (k.clone(), v.clone());
                }
                let op = inner.operators.get(k).expect("Operator not found");
                let next = op.next(v);
                (k.clone(), next)
            })
            .collect::<IndexMap<String, Bson>>();
        inner.group_values = next_map;
        Ok(VmExternalFuncStatus::Continue)
    }

    fn is_completed(&self) -> bool {
        true
    }
}
