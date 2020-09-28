use std::fmt;
use crate::vm::{VM, VmState};
use crate::bson::Value;
use crate::DbErr;

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
    pub fn get(&self) -> &Value {
        self.0.stack_top()
    }

    #[inline]
    pub fn has_error(&self) -> bool {
        self.0.error.is_some()
    }

    #[inline]
    pub fn take_error(&mut self) -> Option<DbErr> {
        Option::take(&mut self.0.error)
    }

    #[inline]
    pub fn step(&mut self) {
        self.0.execute()
    }

}

impl<'a> fmt::Display for DbHandle<'a> {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Program: \n\n{}", self.0.program)
    }

}
