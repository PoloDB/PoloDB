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


use crate::vm::label::Label;

pub(crate) struct PipelineItem {
    pub next_label: Label,
    pub complete_label: Option<Label>,
}

pub(crate) struct AggregationCodeGenContext {
    pub items: Vec<PipelineItem>
}

impl Default for AggregationCodeGenContext {
    fn default() -> Self {
        AggregationCodeGenContext {
            items: Vec::default(),
        }
    }
}
