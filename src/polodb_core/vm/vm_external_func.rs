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
use crate::Result;

pub(crate) enum VmExternalFuncStatus {
    Continue,
    Next(Bson),
}

pub(crate) trait VmExternalFunc {
    fn name(&self) -> &str;
    fn call(&self, args: &[Bson]) -> Result<VmExternalFuncStatus>;
    fn is_completed(&self) -> bool;
}
