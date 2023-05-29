/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::vm::label::Label;

pub(crate) struct AggregationCodeGenContext {
    pub pipeline_labels: Vec<Label>
}

impl Default for AggregationCodeGenContext {
    fn default() -> Self {
        AggregationCodeGenContext {
            pipeline_labels: Vec::default(),
        }
    }
}
