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
use crate::{Error, Result};
use crate::vm::vm_external_func::{VmExternalFunc, VmExternalFuncStatus};

pub(crate) struct VmFuncUnset {
    fields: Vec<String>,
}

impl VmFuncUnset {
    pub(crate) fn compile(val: &Bson) -> Result<Box<dyn VmExternalFunc>> {
        let fields = match val {
            Bson::Array(arr) => {
                let mut fields = Vec::new();
                for v in arr {
                    if let Bson::String(s) = v {
                        fields.push(s.clone());
                    } else {
                        return Err(Error::UnknownAggregationOperation("Invalid argument for $unset".to_string()));
                    }
                }
                fields
            }
            Bson::String(s) => vec![s.clone()],
            _ => return Err(Error::UnknownAggregationOperation("Invalid argument for $unset".to_string())),
        };
        Ok(Box::new(VmFuncUnset {
            fields,
        }))
    }
}

impl VmExternalFunc for VmFuncUnset {
    fn name(&self) -> &str {
        "unset"
    }
    fn call(&self, args: &[Bson]) -> Result<VmExternalFuncStatus> {
        let arg0 = &args[0];
        if arg0.as_null().is_some() {
            return Ok(VmExternalFuncStatus::Next(Bson::Null));
        }
        let mut doc = match arg0 {
            Bson::Document(doc) => doc.clone(),
            _ => return Err(Error::UnknownAggregationOperation("Invalid argument for $unset".to_string())),
        };

        for field in &self.fields {
            doc.remove(field);
        }

        Ok(VmExternalFuncStatus::Next(Bson::Document(doc)))
    }
    fn is_completed(&self) -> bool {
        true
    }

}
