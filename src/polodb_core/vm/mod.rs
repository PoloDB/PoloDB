/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
mod op;
mod subprogram;
mod codegen;
mod label;

pub(crate) use subprogram::SubProgram;

use std::vec::Vec;
use std::cmp::Ordering;
use bson::Bson;
use op::DbOp;
use crate::cursor::Cursor;
use crate::{TransactionType, DbResult, DbErr, LsmKv};
use crate::error::{CannotApplyOperationForTypes, mk_field_name_type_unexpected, mk_unexpected_type_for_op};
use std::cell::Cell;
use std::sync::{Arc, Mutex};
use crate::db::SessionInner;

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

pub struct VM {
    kv_engine:           LsmKv,
    session:             Arc<Mutex<SessionInner>>,
    pub(crate) state:    VmState,
    pc:                  *const u8,
    r0:                  i32,  // usually the logic register
    r1:                  Option<Cursor>,
    pub(crate) r2:       i64,  // usually the counter
    r3:                  usize,
    stack:               Vec<Bson>,
    pub(crate) program:  SubProgram,
    rollback_on_drop:    bool,
}

fn generic_cmp(op: DbOp, val1: &Bson, val2: &Bson) -> DbResult<bool> {
    let ord = crate::bson_utils::value_cmp(val1, val2)?;
    let result = matches!((op, ord),
        (DbOp::Equal, Ordering::Equal) |
        (DbOp::Greater, Ordering::Greater) |
        (DbOp::GreaterEqual, Ordering::Equal) |
        (DbOp::GreaterEqual, Ordering::Greater) |
        (DbOp::Less, Ordering::Less) |
        (DbOp::LessEqual, Ordering::Equal) |
        (DbOp::LessEqual, Ordering::Less)
    );
    Ok(result)
}

impl VM {

    pub(crate) fn new(kv_engine: LsmKv, session: Arc<Mutex<SessionInner>>, program: SubProgram) -> VM {
        let stack = Vec::with_capacity(STACK_SIZE);
        let pc = program.instructions.as_ptr();
        VM {
            kv_engine,
            session,
            state: VmState::Init,
            pc,
            r0: 0,
            r1: None,
            r2: 0,
            r3: 0,
            stack,
            program,
            rollback_on_drop: false,
        }
    }

    fn auto_start_transaction(&mut self, ty: TransactionType) -> DbResult<()> {
        let mut session = self.session.lock()?;
        session.kv_session.start_transaction(ty)
    }

    fn open_read(&mut self, prefix: Bson) -> DbResult<()> {
        self.auto_start_transaction(TransactionType::Read)?;
        let cursor = {
            let session = self.session.lock()?;
            self.kv_engine.open_multi_cursor(Some(&session.kv_session))
        };
        self.r1 = Some(Cursor::new(prefix, cursor));
        Ok(())
    }

    fn open_write(&mut self, prefix: Bson) -> DbResult<()> {
        self.auto_start_transaction(TransactionType::Write)?;
        let cursor = {
            let session = self.session.lock()?;
            self.kv_engine.open_multi_cursor(Some(&session.kv_session))
        };
        self.r1 = Some(Cursor::new(prefix, cursor));
        Ok(())
    }

    fn reset_cursor(&mut self, is_empty: &Cell<bool>) -> DbResult<()> {
        let cursor = self.r1.as_mut().unwrap();
        cursor.reset();
        if cursor.has_next() {
            let item = cursor.peek_data(self.kv_engine.inner.as_ref())?.unwrap();
            let doc = bson::from_slice(item.as_ref())?;
            self.stack.push(Bson::Document(doc));
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

        let result = cursor.reset_by_pkey(op)?;
        if !result {
            return Ok(false);
        }

        let buf = cursor.peek_data(self.kv_engine.inner.as_ref())?.unwrap();
        let doc = bson::from_slice(buf.as_ref())?;
        self.stack.push(Bson::Document(doc));
        Ok(true)
    }

    fn next(&mut self) -> DbResult<()> {
        let cursor = self.r1.as_mut().unwrap();
        cursor.next()?;
        match cursor.peek_data(self.kv_engine.inner.as_ref())? {
            Some(bytes) => {
                let doc = bson::from_slice(bytes.as_ref())?;
                self.stack.push(Bson::Document(doc));

                debug_assert!(self.stack.len() <= 64, "stack too large: {}", self.stack.len());

                self.r0 = 1;
            }

            None => {
                self.r0 = 0;
            }
        }
        Ok(())
    }

    pub(crate) fn stack_top(&self) -> &Bson {
        &self.stack[self.stack.len() - 1]
    }

    #[inline]
    fn reset_location(&mut self, location: u32) {
        unsafe {
            self.pc = self.program.instructions.as_ptr().add(location as usize);
        }
    }

    fn borrow_static(&self, index: usize) -> &Bson {
        &self.program.static_values[index]
    }

    fn inc_numeric(key: &str, a: &Bson, b: &Bson) -> DbResult<Bson> {
        let val = match (a, b) {
            (Bson::Int32(a), Bson::Int32(b)) => Bson::Int32(*a + *b),
            (Bson::Int32(a), Bson::Int64(b)) => Bson::Int64(*a as i64 + *b),
            (Bson::Int32(a), Bson::Double(b)) => Bson::Double(*a as f64 + *b),
            (Bson::Int64(a), Bson::Int64(b)) => Bson::Int64(*a + *b),
            (Bson::Int64(a), Bson::Int32(b)) => Bson::Int64(*a + *b as i64),
            (Bson::Int64(a), Bson::Double(b)) => Bson::Double(*a as f64 + *b),
            (Bson::Double(a), Bson::Double(b)) => Bson::Double(*a + *b),
            (Bson::Double(a), Bson::Int32(b)) => Bson::Double(*a + *b as f64),
            (Bson::Double(a), Bson::Int64(b)) => Bson::Double(*a + *b as f64),

            _ => {
                return Err(DbErr::CannotApplyOperation(Box::new(CannotApplyOperationForTypes {
                    op_name: "$inc".into(),
                    field_name: key.into(),
                    field_type: a.to_string(),
                    target_type: b.to_string(),
                })));
            }
        };
        Ok(val)
    }

    fn mul_numeric(key: &str, a: &Bson, b: &Bson) -> DbResult<Bson> {
        let val = match (a, b) {
            (Bson::Int32(a), Bson::Int32(b)) => Bson::Int32(*a * *b),
            (Bson::Int32(a), Bson::Int64(b)) => Bson::Int64(*a as i64 * *b),
            (Bson::Int32(a), Bson::Double(b)) => Bson::Double(*a as f64 * *b),
            (Bson::Int64(a), Bson::Int64(b)) => Bson::Int64(*a * *b),
            (Bson::Int64(a), Bson::Int32(b)) => Bson::Int64(*a * *b as i64),
            (Bson::Int64(a), Bson::Double(b)) => Bson::Double(*a as f64 * *b),
            (Bson::Double(a), Bson::Double(b)) => Bson::Double(*a * *b),
            (Bson::Double(a), Bson::Int32(b)) => Bson::Double(*a * *b as f64),
            (Bson::Double(a), Bson::Int64(b)) => Bson::Double(*a * *b as f64),

            _ => {
                return Err(DbErr::CannotApplyOperation(Box::new(CannotApplyOperationForTypes {
                    op_name: "$mul".into(),
                    field_name: key.into(),
                    field_type: a.to_string(),
                    target_type: b.to_string(),
                })));
            }
        };
        Ok(val)
    }

    fn inc_field(&mut self, field_id: usize) -> DbResult<()> {
        let key = self.program.static_values[field_id].as_str().unwrap();

        let value_index = self.stack.len() - 1;
        let doc_index = self.stack.len() - 2;

        let value = self.stack[value_index].clone();

        let mut_doc = self.stack[doc_index].as_document_mut().unwrap();

        match mut_doc.get(key) {
            Some(Bson::Null) => {
                return Err(DbErr::IncrementNullField);
            }

            Some(original_value) => {
                let result = VM::inc_numeric(key, original_value, &value)?;
                mut_doc.insert::<String, Bson>(key.into(), result);
            }

            None => {
                mut_doc.insert::<String, Bson>(key.into(), value);
            }

        }
        Ok(())
    }

    fn mul_field(&mut self, field_id: usize) -> DbResult<()> {
        let key = self.program.static_values[field_id].as_str().unwrap();

        let value_index = self.stack.len() - 1;
        let doc_index = self.stack.len() - 2;

        let value = self.stack[value_index].clone();

        let mut_doc = self.stack[doc_index].as_document_mut().unwrap();

        match mut_doc.get(key) {
            Some(original_value) => {
                let new_value = VM::mul_numeric(key, original_value, &value)?;
                mut_doc.insert::<String, Bson>(key.into(), new_value);
            }

            None => {
                mut_doc.insert::<String, Bson>(key.into(), value);
            }

        }
        Ok(())
    }

    fn unset_field(&mut self, field_id: u32) -> DbResult<()> {
        let key = self.program.static_values[field_id as usize].as_str().unwrap();

        let doc_index = self.stack.len() - 1;
        let mut_doc = self.stack[doc_index].as_document_mut().unwrap();

        let _ = mut_doc.remove(key);

        Ok(())
    }

    fn array_size(&mut self) -> DbResult<usize> {
        let top = self.stack.len() - 1;
        let doc = crate::try_unwrap_array!("ArraySize", &self.stack[top]);
        Ok(doc.len())
    }

    fn array_push(&mut self) -> DbResult<()> {
        let st = self.stack.len();
        let val = self.stack[st - 1].clone();
        let array_value = match &mut self.stack[st - 2] {
            Bson::Array(arr) => arr,
            _ => {
                let name = format!("{}", self.stack[st-  2]);
                return Err(DbErr::UnexpectedTypeForOp(mk_unexpected_type_for_op(
                    "$push", "Array", name
                )))
            }
        };
        array_value.push(val);
        Ok(())
    }

    fn array_pop_first(&mut self) -> DbResult<()> {
        let st = self.stack.len();
        let array_value = match &mut self.stack[st - 1] {
            Bson::Array(arr) => arr,
            _ => {
                let name = format!("{}", self.stack[st - 1]);
                return Err(DbErr::UnexpectedTypeForOp(mk_unexpected_type_for_op(
                    "$pop", "Array", name
                )))
            }
        };
        array_value.drain(0..1);

        Ok(())
    }

    fn array_pop_last(&mut self) -> DbResult<()> {
        let st = self.stack.len();
        let array_value = match &mut self.stack[st - 1] {
            Bson::Array(arr) => arr,
            _ => {
                let name = format!("{}", self.stack[st - 1]);
                return Err(DbErr::UnexpectedTypeForOp(mk_unexpected_type_for_op(
                    "$pop", "Array", name
                )))
            }
        };
        array_value.pop();

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

                    DbOp::Label => {
                        self.pc = self.pc.add(5);
                    }

                    DbOp::IncR2 => {
                        self.r2 += 1;
                        self.pc = self.pc.add(1);
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

                    DbOp::PushR0 => {
                        self.stack.push(Bson::from(self.r0));
                        self.pc = self.pc.add(1);
                    }

                    DbOp::StoreR0 => {
                        let top = self.stack_top().as_i64().unwrap();
                        self.r0 = top as i32;
                        self.pc = self.pc.add(1);
                    }

                    DbOp::GetField => {
                        let key_stat_id = self.pc.add(1).cast::<u32>().read();
                        let location = self.pc.add(5).cast::<u32>().read();

                        let key = self.borrow_static(key_stat_id as usize);
                        let key_name = key.as_str().unwrap();
                        let top = self.stack[self.stack.len() - 1].clone();
                        let doc = match top {
                            Bson::Document(doc) => doc,
                            _ => {
                                let name = format!("{}", top);
                                let err = mk_field_name_type_unexpected(
                                    key_name.into(),
                                    "Document".into(),
                                    name);
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

                        let key = self.program.static_values[filed_id as usize].as_str().unwrap();

                        let value_index = self.stack.len() - 1;
                        let doc_index = self.stack.len() - 2;

                        let value = self.stack[value_index].clone();

                        let mut_doc = self.stack[doc_index].as_document_mut().unwrap();

                        mut_doc.insert::<String, Bson>(key.into(), value);

                        self.pc = self.pc.add(5);
                    }

                    DbOp::ArraySize => {
                        let size = try_vm!(self, self.array_size());

                        self.stack.push(Bson::from(size as i64));

                        self.pc = self.pc.add(1);
                    }

                    DbOp::ArrayPush => {
                        try_vm!(self, self.array_push());

                        self.pc = self.pc.add(1);
                    }

                    DbOp::ArrayPopFirst => {
                        try_vm!(self, self.array_pop_first());

                        self.pc = self.pc.add(1);
                    }

                    DbOp::ArrayPopLast => {
                        try_vm!(self, self.array_pop_last());

                        self.pc = self.pc.add(1);
                    }

                    DbOp::UpdateCurrent => {
                        let top_index = self.stack.len() - 1;
                        let top_value = &self.stack[top_index];

                        let doc = top_value.as_document().unwrap();

                        self.r1.as_mut().unwrap().update_current(doc)?;

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

                    DbOp::Equal | DbOp::Greater | DbOp::GreaterEqual |
                    DbOp::Less | DbOp::LessEqual => {
                        let val1 = &self.stack[self.stack.len() - 2];
                        let val2 = &self.stack[self.stack.len() - 1];

                        let cmp = try_vm!(self, generic_cmp(op, val1, val2));

                        self.r0 = if cmp {
                            1
                        } else {
                            0
                        };

                        self.pc = self.pc.add(1);
                    }

                    // stack
                    // -1: Array
                    // -2: value
                    //
                    // check value in Array
                    DbOp::In => {
                        let top1 = &self.stack[self.stack.len() - 1];
                        let top2 = &self.stack[self.stack.len() - 2];

                        self.r0 = 0;

                        for item in top1.as_array().unwrap().iter() {
                            let cmp_result = crate::bson_utils::value_cmp(top2, item);
                            if let Ok(Ordering::Equal) = cmp_result {
                                self.r0 = 1;
                                break;
                            }
                        }

                        self.pc = self.pc.add(1);
                    }

                    DbOp::OpenRead => {
                        let prefix_idx = self.pc.add(1).cast::<u32>().read();
                        let prefix = self.program.static_values[prefix_idx as usize].clone();

                        try_vm!(self, self.open_read(prefix));

                        self.pc = self.pc.add(5);
                    }

                    DbOp::OpenWrite => {
                        let prefix_idx = self.pc.add(1).cast::<u32>().read();
                        let prefix = self.program.static_values[prefix_idx as usize].clone();

                        try_vm!(self, self.open_write(prefix));

                        self.pc = self.pc.add(5);
                    }

                    DbOp::ResultRow => {
                        self.pc = self.pc.add(1);
                        self.state = VmState::HasRow;
                        return Ok(());
                    }

                    DbOp::Close => {
                        self.r1 = None;
                        // TODO: FIXME
                        // if self.rollback_on_drop {
                        //     self.session.auto_commit()?;
                        //     self.rollback_on_drop = false;
                        // }

                        self.pc = self.pc.add(1);
                    }

                    DbOp::SaveStackPos => {
                        self.r3 = self.stack.len();
                        self.pc = self.pc.add(1);
                    }

                    DbOp::RecoverStackPos => {
                        self.stack.resize(self.r3, Bson::Null);
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
        // TODO: FIXME
        // self.session.auto_commit()?;
        self.rollback_on_drop = false;
        Ok(())
    }

    pub(crate) fn set_rollback_on_drop(&mut self, value: bool) {
        self.rollback_on_drop = value;
    }

}

impl Drop for VM {

    fn drop(&mut self) {
        if self.rollback_on_drop {
            // TODO: FIXME
            // let _result = self.session.rollback();
            // #[cfg(debug_assertions)]
            // if let Err(err) = _result {
            //     panic!("rollback fatal: {}", err);
            // }
        }
    }

}
