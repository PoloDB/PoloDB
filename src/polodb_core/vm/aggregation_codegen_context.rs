/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

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
