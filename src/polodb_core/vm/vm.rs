/*
 * Copyright (c) 2020 Vincent Chan
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation; either version 3, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */
use std::vec::Vec;
// use super::op::DbOp;
use super::subprogram::SubProgram;
use crate::bson::Value;
use std::ptr::null;

const STACK_SIZE: usize = 256;

#[repr(i8)]
pub enum VmState {
    Reject = -1,
    Init = 0,
    Running = 1,
    Resolve = 2,
}

pub struct VM {
    state:    VmState,
    pc:       *const u8,
    r0:       i32,
    stack:    Vec<Value>,
    program:  Box<SubProgram>,
}

impl VM {

    pub(crate) fn new(program: Box<SubProgram>) -> VM {
        let mut stack = Vec::new();
        stack.resize(STACK_SIZE, Value::Null);
        VM {
            state: VmState::Init,
            pc: null(),
            r0: 0,
            stack,
            program,
        }
    }

    pub(crate) fn execute(&mut self) {
        unimplemented!()
    }

}
