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

mod op;
mod subprogram;
mod codegen;
mod label;
mod vm;
mod global_variable;
mod aggregation_codegen_context;
mod vm_external_func;
mod vm_count;
mod vm_group;
mod operators;
mod vm_skip;
mod vm_sort;
mod vm_limit;
mod vm_unset;
mod vm_add_fields;

pub(crate) use subprogram::SubProgram;
pub(crate) use vm::{VM, VmState};
