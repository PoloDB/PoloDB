/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::fmt;
use std::marker::PhantomData;
use bson::Bson;
use serde::de::DeserializeOwned;
use crate::{ClientSession, DbResult};
use crate::session::SessionInner;
use crate::vm::{VM, VmState};

/// A `ClientCursor` is used get the result of a query.
/// You can move the cursor forward using the `advance()`.
///
/// Additionally, you can use deserialize_current() method to
/// deserialize the documents returned by advance()
pub struct ClientCursor<T: DeserializeOwned> {
    vm: VM,
    session: SessionInner,
    _phantom: PhantomData<T>,
}

impl<T: DeserializeOwned> ClientCursor<T> {

    pub(crate) fn new(vm: VM, session: SessionInner) -> ClientCursor<T> {
        ClientCursor{
            vm,
            session,
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

    pub fn advance(&mut self) -> DbResult<bool> {
        self.vm.execute(&mut self.session)?;
        Ok(self.has_row())
    }

    pub fn deserialize_current(&self) -> DbResult<T> {
        let result: T = bson::from_bson(self.get().clone())?;
        Ok(result)
    }

}

impl<T: DeserializeOwned> fmt::Display for ClientCursor<T> {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Program: \n\n{}", self.vm.program)
    }

}

impl<T> Iterator for ClientCursor<T>
    where
        T: DeserializeOwned + Unpin + Send + Sync,
{
    type Item = DbResult<T>;

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

/// A `ClientSessionCursor` is used get the result of a query.
pub struct ClientSessionCursor<T: DeserializeOwned> {
    vm: VM,
    _phantom: PhantomData<T>,
}

impl<T: DeserializeOwned> ClientSessionCursor<T> {

    pub(crate) fn new(vm: VM) -> ClientSessionCursor<T> {
        ClientSessionCursor{
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

    pub fn advance(&mut self, session: &mut ClientSession) -> DbResult<bool> {
        self.advance_inner(&mut session.inner)
    }

    #[inline]
    pub(crate) fn advance_inner(&mut self, session: &mut SessionInner) -> DbResult<bool> {
        self.vm.execute(session)?;
        Ok(self.has_row())
    }

    pub fn deserialize_current(&self) -> DbResult<T> {
        let result: T = bson::from_bson(self.get().clone())?;
        Ok(result)
    }

    pub fn iter<'c, 's>(&'c mut self, session: &'s mut ClientSession) -> ClientSessionCursorIter<'s, 'c, T> {
        ClientSessionCursorIter {
            cursor: self,
            session,
        }
    }

}

impl<T: DeserializeOwned> fmt::Display for ClientSessionCursor<T> {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Program: \n\n{}", self.vm.program)
    }

}

pub struct ClientSessionCursorIter<'s, 'c, T: DeserializeOwned> {
    cursor: &'c mut ClientSessionCursor<T>,
    session: &'s mut ClientSession,
}

impl<T> Iterator for ClientSessionCursorIter<'_, '_, T>
    where
        T: DeserializeOwned + Unpin + Send + Sync,
{
    type Item = DbResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let test = self.cursor.advance(self.session);
        match test {
            Ok(false) => None,
            Ok(true) => {
                Some(Ok(bson::from_bson(self.cursor.get().clone()).unwrap()))
            }
            Err(err) =>{
                Some(Err(err))
            }
        }
    }
}
