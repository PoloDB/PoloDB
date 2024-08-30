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

#[derive(Debug, Clone)]
pub struct UpdateOptions {
    pub upsert: Option<bool>,
}

impl UpdateOptions {
    pub fn builder() -> UpdateOptionsBuilder {
        UpdateOptionsBuilder::default()
    }

    pub(crate) fn is_upsert(&self) -> bool {
        self.upsert.unwrap_or(false)
    }
}

pub struct UpdateOptionsBuilder {
    upsert: Option<bool>,
}

impl UpdateOptionsBuilder {
    pub fn upsert(mut self, upsert: bool) -> Self {
        self.upsert = Some(upsert);
        self
    }

    pub fn build(self) -> UpdateOptions {
        UpdateOptions {
            upsert: self.upsert,
        }
    }
}

impl Default for UpdateOptionsBuilder {
    fn default() -> Self {
        UpdateOptionsBuilder { upsert: None }
    }
}

impl Default for UpdateOptions {
    fn default() -> Self {
        UpdateOptions { upsert: None }
    }
}
