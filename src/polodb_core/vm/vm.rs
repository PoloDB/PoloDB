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
use std::cmp::Ordering;
use super::subprogram::SubProgram;
use super::op::DbOp;
use crate::bson::Value;
use crate::cursor::Cursor;
use crate::page::PageHandler;
use crate::btree::{HEADER_SIZE, ITEM_SIZE};
use crate::DbErr;

const STACK_SIZE: usize = 256;

macro_rules! try_vm {
    ($self:ident, $action:expr) => {
        match $action {
            Ok(result) => result,
            Err(err) => {
                $self.error = Some(err);
                $self.state = VmState::Halt;
                return;
            }
        }
    }
}

#[repr(i8)]
#[derive(PartialEq, Copy, Clone)]
pub enum VmState {
    Halt = -1,
    Init = 0,
    Running = 1,
    HasRow = 2,
}

pub struct VM<'a> {
    pub(crate) state:    VmState,
    pc:       *const u8,
    r0:       i32,
    r1:       Option<Box<Cursor>>,
    page_handler: &'a mut PageHandler,
    stack:    Vec<Value>,
    program:  Box<SubProgram>,
    pub(crate) error:    Option<DbErr>,
}

impl<'a> VM<'a> {

    pub(crate) fn new(page_handler: &mut PageHandler, program: Box<SubProgram>) -> VM {
        let stack = Vec::with_capacity(STACK_SIZE);
        let pc = program.instructions.as_ptr();
        VM {
            state: VmState::Init,
            pc,
            r0: 0,
            r1: None,
            page_handler,
            stack,
            program,
            error: None,
        }
    }

    #[inline]
    fn item_size(&self) -> u32 {
        (self.page_handler.page_size - HEADER_SIZE) / ITEM_SIZE
    }

    fn open_read(&mut self, root_pid: u32) {
        self.r1 = Some(Box::new(Cursor::new(self.item_size(), root_pid)));
    }

    fn reset_cursor(&mut self) {
        try_vm!(self, self.r1.as_mut().unwrap().reset(self.page_handler))
    }

    fn next(&mut self) {
        let result = try_vm!(self, self.r1.as_mut().unwrap().next(self.page_handler));
        match &result {
            Some(doc) => {
                self.stack.push(Value::Document(doc.clone()));

                #[cfg(debug_assertions)]
                if self.stack.len() > 64 {
                    eprintln!("stack too large: {}", self.stack.len());
                }

                self.r0 = 1;
            }

            None => {
                self.r0 = 0;
            }
        }
    }

    pub(crate) fn stack_top(&self) -> &Value {
        &self.stack[self.stack.len() - 1]
    }

    #[inline]
    fn reset_location(&mut self, location: u32) {
        unsafe {
            self.pc = self.program.instructions.as_ptr().add(location as usize);
        }
    }

    pub(crate) fn execute(&mut self) {
        if self.state == VmState::Halt {
            panic!("vm is halt, can not execute");
        }
        self.state = VmState::Running;
        unsafe {
            loop {
                let op = self.pc.cast::<DbOp>().read();
                match op {
                    DbOp::Goto => {
                        let location = self.pc.add(1).cast::<u32>().read();
                        self.reset_location(location);
                    }

                    DbOp::TrueJump => {
                        let location = self.pc.add(1).cast::<u32>().read();
                        if self.r0 != 0 {  // true
                            self.reset_location(location);
                        } else {
                            self.pc = self.pc.add(5);
                        }
                    }

                    DbOp::FalseJump => {
                        let location = self.pc.add(1).cast::<u32>().read();
                        if self.r0 == 0 {  // false
                            self.reset_location(location);
                        } else {
                            self.pc = self.pc.add(5);
                        }
                    }

                    DbOp::Rewind => {
                        self.reset_cursor();
                        if self.error.is_some() {
                            return;
                        }
                        self.pc = self.pc.add(1);
                    }

                    DbOp::Next => {
                        self.next();
                        if self.error.is_some() {
                            return;
                        }
                        if self.r0 != 0 {
                            let location = self.pc.add(1).cast::<u32>().read();
                            self.reset_location(location);
                        } else {
                            self.pc = self.pc.add(5);
                        }
                    }

                    DbOp::PushValue => {
                        let location = self.pc.add(1).cast::<u32>().read();
                        self.stack.push(self.program.static_values[location as usize].clone());
                        self.pc = self.pc.add(5);
                    }

                    DbOp::GetField => {
                        let key_stat_id = self.pc.add(1).cast::<u32>().read();
                        let location = self.pc.add(5).cast::<u32>().read();

                        let key = self.program.static_values[key_stat_id as usize].unwrap_string();
                        let top = self.stack[self.stack.len()].clone();
                        let doc = top.unwrap_document();

                        match doc.get(key) {
                            Some(val) => {
                                self.stack.push(val.clone());
                                self.pc = self.pc.add(9);
                            }

                            None => {
                                self.reset_location(location);
                            }

                        }

                    }

                    DbOp::Pop => {
                        self.stack.pop();
                        self.pc = self.pc.add(1);
                    }

                    DbOp::Equal => {
                        let top1 = &self.stack[self.stack.len() - 1];
                        let top2 = &self.stack[self.stack.len() - 2];

                        match top1.value_cmp(top2) {
                            Ok(Ordering::Equal) => {
                                self.r0 = 1;
                            }

                            Ok(_) => {
                                self.r0 = 0;
                            }

                            Err(DbErr::TypeNotComparable(_, _)) => {
                                self.r0 = -1;
                            }

                            Err(err) => {
                                self.error = Some(err);
                                return;
                            }

                        }

                        self.pc = self.pc.add(1);
                    }

                    DbOp::Cmp => {
                        let top1 = &self.stack[self.stack.len() - 1];
                        let top2 = &self.stack[self.stack.len() - 2];

                        match top1.value_cmp(top2) {
                            Ok(Ordering::Greater) => {
                                self.r0 = 1;
                            }

                            Ok(Ordering::Less) => {
                                self.r0 = -1;
                            }

                            Ok(Ordering::Equal) => {
                                self.r0 = 0;
                            }

                            Err(err) => {
                                self.error = Some(err);
                                return;
                            }
                        }

                        self.pc = self.pc.add(1);
                    }

                    DbOp::OpenRead => {
                        let root_pid = self.pc.add(1).cast::<u32>().read();

                        self.open_read(root_pid);

                        if self.error.is_some() {
                            return;
                        }

                        self.pc = self.pc.add(5);
                    }

                    DbOp::ResultRow => {
                        self.pc = self.pc.add(1);
                        self.state = VmState::HasRow;
                        return;
                    }

                    DbOp::Close => {
                        self.r1 = None;

                        self.pc = self.pc.add(1);
                    }

                    DbOp::_EOF |
                    DbOp::Halt => {
                        self.r1 = None;
                        self.state = VmState::Halt;
                        return;
                    }

                }
            }
        }
    }

}
