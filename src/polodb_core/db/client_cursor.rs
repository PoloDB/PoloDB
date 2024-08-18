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

use std::fmt;
use std::marker::PhantomData;
use bson::Bson;
use serde::de::DeserializeOwned;
use crate::{Result};
use crate::vm::{VM, VmState};

/// A `ClientCursor` is used get the result of a query.
/// You can move the cursor forward using the `advance()`.
///
/// Additionally, you can use deserialize_current() method to
/// deserialize the documents returned by advance()
pub struct ClientCursor<T: DeserializeOwned + Send + Sync> {
    vm: VM,
    _phantom: PhantomData<T>,
}

impl<T: DeserializeOwned + Send + Sync> ClientCursor<T> {

    pub(crate) fn new(vm: VM) -> ClientCursor<T> {
        ClientCursor{
            vm,
            _phantom: Default::default(),
        }
    }

    #[inline]
    fn has_row(&self) -> bool {
        self.vm.state == VmState::HasRow
    }

    #[inline]
    pub(crate) fn get(&self) -> &Bson {
        self.vm.stack_top()
    }

    pub fn advance(&mut self) -> Result<bool> {
        if self.vm.state == VmState::Halt {
            return Ok(false);
        }
        self.vm.execute()?;
        Ok(self.has_row())
    }

    pub fn deserialize_current(&self) -> Result<T> {
        let result: T = bson::from_bson(self.get().clone())?;
        Ok(result)
    }

}

impl<T: DeserializeOwned + Send + Sync> fmt::Display for ClientCursor<T> {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Program: \n\n{}", self.vm.program)
    }

}

impl<T> Iterator for ClientCursor<T>
    where
        T: DeserializeOwned + Unpin + Send + Sync,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let test = self.advance();
        match test {
            Ok(false) => None,
            Ok(true) => {
                Some(Ok(bson::from_bson(self.get().clone()).unwrap()))
            }
            Err(err) =>{
                Some(Err(err))
            }
        }
    }
}
