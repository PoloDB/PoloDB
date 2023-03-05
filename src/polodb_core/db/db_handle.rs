/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::fmt;
use bson::Bson;
use crate::vm::{VM, VmState};
use crate::DbResult;

/**
 * A VM wrapper for Rust-level API
 */
pub struct DbHandle<'a>(VM<'a>);

impl<'a> DbHandle<'a> {

    pub fn new(vm: VM) -> DbHandle {
        DbHandle(vm)
    }

    #[inline]
    pub fn has_row(&self) -> bool {
        self.0.state == VmState::HasRow
    }

    #[inline]
    pub fn state(&self) -> i8 {
        let state = self.0.state;
        state as i8
    }

    #[inline]
    pub fn get(&self) -> &Bson {
        self.0.stack_top()
    }

    #[inline]
    pub fn step(&mut self) -> DbResult<()> {
        self.0.execute()
    }

    #[inline]
    pub fn commit_and_close_vm(self) -> DbResult<()> {
        self.0.commit_and_close()
    }

    #[inline]
    pub fn set_rollback_on_drop(&mut self, value: bool) {
        self.0.set_rollback_on_drop(value)
    }

}

impl<'a> fmt::Display for DbHandle<'a> {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Program: \n\n{}", self.0.program)
    }

}
