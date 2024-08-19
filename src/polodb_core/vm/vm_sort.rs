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

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use bson::{Bson, Document};
use crate::vm::vm_external_func::{VmExternalFunc, VmExternalFuncStatus};
use crate::{Result, Error};

pub(crate) struct VmFuncSort {
    order_map: HashMap<String, i8>,
    buffer: RefCell<Vec<Document>>,
    idx: AtomicUsize,
}

impl VmFuncSort {
    pub(crate) fn compile(val: &Bson) -> Result<Box<dyn VmExternalFunc>> {
        let order_map = match val {
            Bson::Document(doc) => {
                let mut result = HashMap::default();
                for (k, v) in doc.iter() {
                    let order = match v {
                        Bson::Int32(val) => *val as i8,
                        Bson::Int64(val) => *val as i8,
                        _ => return Err(Error::ValidationError("Invalid sort value".into()))
                    };
                    result.insert(k.clone(), order);
                }
                result
            }
            _ => return Err(Error::ValidationError("Invalid sort value".into()))
        };
        let result = VmFuncSort {
            order_map,
            buffer: RefCell::new(Vec::default()),
            idx: AtomicUsize::new(0),
        };
        Ok(Box::new(result))
    }

    fn i8_to_ordering(i: i8) -> std::cmp::Ordering {
        match i {
            1 => std::cmp::Ordering::Less,
            -1 => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        }
    }

    fn sort_array(&self) {
        let mut array = self.buffer.borrow_mut();
        array.sort_by(|a, b| {
            for (k, v) in self.order_map.iter() {
                let a_val = a.get(k);
                let b_val = b.get(k);
                match (a_val, b_val) {
                    (Some(a_val), Some(b_val)) => {
                        let result =  crate::utils::bson::value_cmp(a_val, b_val).expect("Invalid sort value");
                        match result {
                            std::cmp::Ordering::Equal => continue,
                            std::cmp::Ordering::Less => return Self::i8_to_ordering(v.clone()),
                            std::cmp::Ordering::Greater => return Self::i8_to_ordering(-v.clone()),
                        }
                    }
                    (Some(_), None) => return Self::i8_to_ordering(v.clone()),
                    (None, Some(_)) => return Self::i8_to_ordering(-v.clone()),
                    (None, None) => continue,
                }
            }
            std::cmp::Ordering::Equal
        });
    }
}

impl VmExternalFunc for VmFuncSort {
    fn name(&self) -> &str {
        "sort"
    }

    fn call(&self, args: &[Bson]) -> Result<VmExternalFuncStatus> {
        let arg0 = &args[0];
        match arg0 {
            Bson::Document(doc) => {
                let mut buffer = self.buffer.borrow_mut();
                buffer.push(doc.clone());
                Ok(VmExternalFuncStatus::Continue)
            }
            Bson::Null => {
                self.sort_array();
                let next = {
                    let idx = self.idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let buffer = self.buffer.borrow();
                    if idx >= buffer.len() {
                        Bson::Null
                    } else {
                        buffer[idx].clone().into()
                    }
                };
                Ok(VmExternalFuncStatus::Next(next))
            }
            _ => {
                Err(Error::ValidationError("Invalid sort value".into()))
            }
        }
    }

    fn is_completed(&self) -> bool {
        let idx = self.idx.load(std::sync::atomic::Ordering::Relaxed);
        let buffer = self.buffer.borrow();
        idx >= buffer.len()
    }
}
