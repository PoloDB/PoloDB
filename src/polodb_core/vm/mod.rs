mod op;
mod subprogram;
mod codegen;

pub(crate) use subprogram::SubProgram;

use std::rc::Rc;
use std::vec::Vec;
use std::cmp::Ordering;
use polodb_bson::Value;
use polodb_bson::error::BsonErr;
use op::DbOp;
use crate::cursor::Cursor;
use crate::page::PageHandler;
use crate::btree::{HEADER_SIZE, ITEM_SIZE};
use crate::{TransactionType, DbResult, DbErr};
use crate::error::mk_field_name_type_unexpected;
use std::cell::Cell;

const STACK_SIZE: usize = 256;

macro_rules! try_vm {
    ($self:ident, $action:expr) => {
        match $action {
            Ok(result) => result,
            Err(err) => {
                $self.state = VmState::Halt;
                return Err(err);
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
    pc:                  *const u8,
    r0:                  i32,  // usually the logic register
    r1:                  Option<Box<Cursor>>,
    pub(crate) r2:       i64,  // usually the counter
    r3:                  usize,
    page_handler:        &'a mut PageHandler,
    stack:               Vec<Value>,
    pub(crate) program:  Box<SubProgram>,
    rollback_on_drop:    bool,
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
            r2: 0,
            r3: 0,
            page_handler,
            stack,
            program,
            rollback_on_drop: false,
        }
    }

    #[inline]
    fn item_size(&self) -> u32 {
        (self.page_handler.page_size - HEADER_SIZE) / ITEM_SIZE
    }

    fn auto_start_transaction(&mut self, ty: TransactionType) -> DbResult<()> {
        let result = self.page_handler.auto_start_transaction(ty)?;
        if result.auto_start {
            self.rollback_on_drop = true;
        }
        Ok(())
    }

    fn open_read(&mut self, root_pid: u32) -> DbResult<()> {
        self.auto_start_transaction(TransactionType::Read)?;
        self.r1 = Some(Box::new(Cursor::new(self.item_size(), root_pid)));
        Ok(())
    }

    fn open_write(&mut self, root_pid: u32) -> DbResult<()> {
        self.auto_start_transaction(TransactionType::Write)?;
        self.r1 = Some(Box::new(Cursor::new(self.item_size(), root_pid)));
        Ok(())
    }

    fn reset_cursor(&mut self, is_empty: &Cell<bool>) -> DbResult<()> {
        let cursor = self.r1.as_mut().unwrap();
        cursor.reset(self.page_handler)?;
        if cursor.has_next() {
            let item = cursor.peek().unwrap();
            let doc = self.page_handler.get_doc_from_ticket(&item)?.unwrap();
            self.stack.push(Value::Document(doc));
            is_empty.set(false);
        } else {
            is_empty.set(true);
        }
        Ok(())
    }

    fn find_by_primary_key(&mut self) -> DbResult<bool> {
        let cursor = self.r1.as_mut().unwrap();

        let top_index = self.stack.len() - 1;
        let op = &self.stack[top_index];

        let result = cursor.reset_by_pkey(self.page_handler, op)?;
        if !result {
            return Ok(false);
        }

        let ticket = cursor.peek().unwrap();
        let doc = self.page_handler.get_doc_from_ticket(&ticket)?;
        if let Some(doc) = doc {
            self.stack.push(Value::Document(doc));
            Ok(true)
        } else {
            panic!("unexpected: item with key '{}' has been deleted, pid: {}, index: {}", op, ticket.pid, ticket.index);
        }
    }

    fn next(&mut self) -> DbResult<()> {
        let cursor = self.r1.as_mut().unwrap();
        let _ = cursor.next(self.page_handler)?;
        match cursor.peek() {
            Some(ticket) => {
                let doc = self.page_handler.get_doc_from_ticket(&ticket)?.unwrap();
                self.stack.push(Value::Document(doc));

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
        Ok(())
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

    fn borrow_static(&self, index: usize) -> &Value {
        &self.program.static_values[index]
    }

    fn inc_field(&mut self, field_id: usize) -> DbResult<()> {
        let key = self.program.static_values[field_id].unwrap_string();

        let value_index = self.stack.len() - 1;
        let doc_index = self.stack.len() - 2;

        let value = self.stack[value_index].clone();

        let doc = self.stack[doc_index].unwrap_document_mut();
        let mut_doc = Rc::make_mut(doc);


        match mut_doc.get(key) {
            Some(Value::Null) => {
                return Err(DbErr::IncrementNullField);
            }

            Some(Value::Int(original_int_value)) => {
                let new_value = match value {
                    Value::Int(inc_int_value) => {
                        let new_value = *original_int_value + inc_int_value;
                        Value::Int(new_value)
                    }

                    Value::Double(inc_double_value) => {
                        let new_value = *original_int_value as f64 + inc_double_value;
                        Value::Double(new_value)
                    }

                    _ => {
                        return Err(mk_field_name_type_unexpected(key, "number", value.ty_name()));
                    }
                };
                mut_doc.insert(key.into(), new_value);
            }

            Some(Value::Double(original_float_value)) => {
                let new_value = match value {
                    Value::Int(inc_int_value) => {
                        let new_value = *original_float_value + inc_int_value as f64;
                        Value::Double(new_value)
                    }

                    Value::Double(inc_float_value) => {
                        let new_value = *original_float_value + inc_float_value;
                        Value::Double(new_value)
                    }

                    _ => {
                        return Err(mk_field_name_type_unexpected(key, "number", value.ty_name()));
                    }

                };
                mut_doc.insert(key.into(), new_value);
            }

            Some(ty) => {
                return Err(mk_field_name_type_unexpected(key, "number", ty.ty_name()));
            }

            None => {
                mut_doc.insert(key.into(), value);
            }

        }
        Ok(())
    }

    fn mul_field(&mut self, field_id: usize) -> DbResult<()> {
        let key = self.program.static_values[field_id].unwrap_string();

        let value_index = self.stack.len() - 1;
        let doc_index = self.stack.len() - 2;

        let value = self.stack[value_index].clone();

        let doc = self.stack[doc_index].unwrap_document_mut();
        let mut_doc = Rc::make_mut(doc);

        match mut_doc.get(key) {
            Some(Value::Int(original_int_value)) => {
                let new_value = match value {
                    Value::Int(inc_int_value) => {
                        let new_value = *original_int_value * inc_int_value;
                        Value::Int(new_value)
                    }

                    Value::Double(inc_double_value) => {
                        let new_value = *original_int_value as f64 * inc_double_value;
                        Value::Double(new_value)
                    }

                    _ => {
                        return Err(mk_field_name_type_unexpected(key, "number", value.ty_name()));
                    }
                };
                mut_doc.insert(key.into(), new_value);
            }

            Some(Value::Double(original_float_value)) => {
                let new_value = match value {
                    Value::Int(inc_int_value) => {
                        let new_value = *original_float_value * inc_int_value as f64;
                        Value::Double(new_value)
                    }

                    Value::Double(inc_float_value) => {
                        let new_value = *original_float_value * inc_float_value;
                        Value::Double(new_value)
                    }

                    _ => {
                        return Err(mk_field_name_type_unexpected(key, "number", value.ty_name()));
                    }

                };
                mut_doc.insert(key.into(), new_value);
            }

            Some(ty) => {
                return Err(mk_field_name_type_unexpected(key, "number", ty.ty_name()));
            }

            None => {
                mut_doc.insert(key.into(), value);
            }

        }
        Ok(())
    }

    fn unset_field(&mut self, field_id: u32) -> DbResult<()> {
        let key = self.program.static_values[field_id as usize].unwrap_string();

        let doc_index = self.stack.len() - 1;
        let doc = self.stack[doc_index].unwrap_document_mut();
        let mut_doc = Rc::make_mut(doc);

        let _ = mut_doc.remove(key);

        Ok(())
    }

    pub(crate) fn execute(&mut self) -> DbResult<()> {
        if self.state == VmState::Halt {
            return Err(DbErr::VmIsHalt);
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

                    DbOp::IfTrue => {
                        let location = self.pc.add(1).cast::<u32>().read();
                        if self.r0 != 0 {  // true
                            self.reset_location(location);
                        } else {
                            self.pc = self.pc.add(5);
                        }
                    }

                    DbOp::IfFalse => {
                        let location = self.pc.add(1).cast::<u32>().read();
                        if self.r0 == 0 {  // false
                            self.reset_location(location);
                        } else {
                            self.pc = self.pc.add(5);
                        }
                    }

                    DbOp::IfGreater => {
                        let location = self.pc.add(1).cast::<u32>().read();
                        if self.r0 > 0 {  // greater
                            self.reset_location(location);
                        } else {
                            self.pc = self.pc.add(5);
                        }
                    }

                    DbOp::IfLess => {
                        let location = self.pc.add(1).cast::<u32>().read();
                        if self.r0 < 0 {  // greater
                            self.reset_location(location);
                        } else {
                            self.pc = self.pc.add(5);
                        }
                    }

                    DbOp::Rewind => {
                        let location = self.pc.add(1).cast::<u32>().read();

                        let is_empty = Cell::new(false);
                        try_vm!(self, self.reset_cursor(&is_empty));

                        if is_empty.get() {
                            self.reset_location(location);
                        } else {
                            self.pc = self.pc.add(5);
                        }
                    }

                    DbOp::FindByPrimaryKey => {
                        let location = self.pc.add(1).cast::<u32>().read();

                        let found = try_vm!(self, self.find_by_primary_key());

                        if !found {
                            self.reset_location(location);
                        } else {
                            self.pc = self.pc.add(5);
                        }
                    }

                    DbOp::Next => {
                        try_vm!(self, self.next());
                        if self.r0 != 0 {
                            let location = self.pc.add(1).cast::<u32>().read();
                            self.reset_location(location);
                        } else {
                            self.pc = self.pc.add(5);
                        }
                    }

                    DbOp::PushValue => {
                        let id = self.pc.add(1).cast::<u32>().read();
                        let value = self.borrow_static(id as usize).clone();
                        self.stack.push(value);
                        self.pc = self.pc.add(5);
                    }

                    DbOp::GetField => {
                        let key_stat_id = self.pc.add(1).cast::<u32>().read();
                        let location = self.pc.add(5).cast::<u32>().read();

                        let key = self.borrow_static(key_stat_id as usize);
                        let key_name = key.unwrap_string();
                        let top = self.stack[self.stack.len() - 1].clone();
                        let doc = match top {
                            Value::Document(doc) => doc,
                            _ => {
                                let err = mk_field_name_type_unexpected(key_name, "Document", top.ty_name());
                                self.state = VmState::Halt;
                                return Err(err)
                            }
                        };

                        match doc.get(key_name) {
                            Some(val) => {
                                self.stack.push(val.clone());
                                self.pc = self.pc.add(9);
                            }

                            None => {
                                self.reset_location(location);
                            }

                        }

                    }

                    DbOp::UnsetField => {
                        let field_id = self.pc.add(1).cast::<u32>().read();

                        try_vm!(self, self.unset_field(field_id));

                        self.pc = self.pc.add(5);
                    }

                    DbOp::IncField => {
                        let filed_id = self.pc.add(1).cast::<u32>().read();

                        try_vm!(self, self.inc_field(filed_id as usize));

                        self.pc = self.pc.add(5);
                    }

                    DbOp::MulField => {
                        let filed_id = self.pc.add(1).cast::<u32>().read();

                        try_vm!(self, self.mul_field(filed_id as usize));

                        self.pc = self.pc.add(5);
                    }

                    DbOp::SetField => {
                        let filed_id = self.pc.add(1).cast::<u32>().read();

                        let key = self.program.static_values[filed_id as usize].unwrap_string();

                        let value_index = self.stack.len() - 1;
                        let doc_index = self.stack.len() - 2;

                        let value = self.stack[value_index].clone();

                        let doc_ref = self.stack[doc_index].unwrap_document_mut();
                        let mut_doc = Rc::make_mut(doc_ref);

                        mut_doc.insert(key.into(), value);

                        self.pc = self.pc.add(5);
                    }

                    DbOp::UpdateCurrent => {
                        let top_index = self.stack.len() - 1;
                        let top_value = &self.stack[top_index];

                        let doc = top_value.unwrap_document();

                        self.r1.as_mut().unwrap().update_current(self.page_handler, doc.as_ref())?;

                        self.pc = self.pc.add(1);
                    }

                    DbOp::Pop => {
                        self.stack.pop();
                        self.pc = self.pc.add(1);
                    }

                    DbOp::Pop2 => {
                        let offset = self.pc.add(1).cast::<u32>().read();

                        self.stack.set_len(self.stack.len() - (offset as usize));

                        self.pc = self.pc.add(5);
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

                            Err(BsonErr::TypeNotComparable(_, _)) => {
                                self.r0 = -1;
                            }

                            Err(err) => {
                                self.state = VmState::Halt;
                                return Err(err.into());
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
                                self.state = VmState::Halt;
                                return Err(err.into());
                            }
                        }

                        self.pc = self.pc.add(1);
                    }

                    // stack
                    // -1: Aarry
                    // -2: value
                    //
                    // check value in Array
                    DbOp::In => {
                        let top1 = &self.stack[self.stack.len() - 1];
                        let top2 = &self.stack[self.stack.len() - 2];

                        self.r0 = 0;

                        for item in top1.unwrap_array().iter() {
                            let cmp_result = top2.value_cmp(item);
                            if let Ok(Ordering::Equal) = cmp_result {
                                self.r0 = 1;
                                break;
                            }
                        }

                        self.pc = self.pc.add(1);
                    }

                    DbOp::OpenRead => {
                        let root_pid = self.pc.add(1).cast::<u32>().read();

                        try_vm!(self, self.open_read(root_pid));

                        self.pc = self.pc.add(5);
                    }

                    DbOp::OpenWrite => {
                        let root_pid = self.pc.add(1).cast::<u32>().read();

                        try_vm!(self, self.open_write(root_pid));

                        self.pc = self.pc.add(5);
                    }

                    DbOp::ResultRow => {
                        self.pc = self.pc.add(1);
                        self.state = VmState::HasRow;
                        return Ok(());
                    }

                    DbOp::Close => {
                        self.r1 = None;
                        if self.rollback_on_drop {
                            self.page_handler.auto_commit()?;
                            self.rollback_on_drop = false;
                        }

                        self.pc = self.pc.add(1);
                    }

                    DbOp::SaveStackPos => {
                        self.r3 = self.stack.len();
                        self.pc = self.pc.add(1);
                    }

                    DbOp::RecoverStackPos => {
                        self.stack.resize(self.r3, Value::Null);
                        self.pc = self.pc.add(1);
                    }

                    DbOp::_EOF |
                    DbOp::Halt => {
                        self.r1 = None;
                        self.state = VmState::Halt;
                        return Ok(());
                    }

                }
            }
        }
    }

    pub(crate) fn commit_and_close(mut self) -> DbResult<()> {
        self.page_handler.auto_commit()?;
        self.rollback_on_drop = false;
        Ok(())
    }

    pub(crate) fn set_rollback_on_drop(&mut self, value: bool) {
        self.rollback_on_drop = value;
    }

}

impl<'a> Drop for VM<'a> {

    fn drop(&mut self) {
        if self.rollback_on_drop {
            let _result = self.page_handler.rollback();
            #[cfg(debug_assertions)]
            if let Err(err) = _result {
                panic!("rollback fatal: {}", err);
            }
        }
    }

}
