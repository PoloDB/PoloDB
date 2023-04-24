/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use bson::Bson;
use std::cell::Cell;
use std::cmp::Ordering;
use crate::{Error, Result, LsmKv, TransactionType};
use crate::cursor::Cursor;
use crate::errors::{CannotApplyOperationForTypes, mk_field_name_type_unexpected, mk_unexpected_type_for_op};
use crate::session::SessionInner;
use crate::vm::op::DbOp;
use crate::vm::SubProgram;

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

const STACK_SIZE: usize = 256;

#[repr(i8)]
#[derive(PartialEq, Copy, Clone)]
pub enum VmState {
    Halt = -1,
    Init = 0,
    Running = 1,
    HasRow = 2,
}

pub(crate) struct VM {
    kv_engine:           LsmKv,
    pub(crate) state:    VmState,
    pc:                  *const u8,
    r0:                  i32,  // usually the logic register
    r1:                  Option<Cursor>,
    pub(crate) r2:       i64,  // usually the counter
    r3:                  usize,
    stack:               Vec<Bson>,
    pub(crate) program:  SubProgram,
}

fn generic_cmp(op: DbOp, val1: &Bson, val2: &Bson) -> Result<bool> {
    let ord = crate::utils::bson::value_cmp(val1, val2)?;
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

    pub(crate) fn new(kv_engine: LsmKv, program: SubProgram) -> VM {
        let stack = Vec::with_capacity(STACK_SIZE);
        let pc = program.instructions.as_ptr();
        VM {
            kv_engine,
            state: VmState::Init,
            pc,
            r0: 0,
            r1: None,
            r2: 0,
            r3: 0,
            stack,
            program,
        }
    }

    fn open_read(&mut self, session: &mut SessionInner, prefix: Bson) -> Result<()> {
        session.auto_start_transaction(TransactionType::Read)?;
        let mut cursor = self.kv_engine.open_multi_cursor(Some(session.kv_session())) ;
        cursor.go_to_min()?;
        self.r1 = Some(Cursor::new(prefix, cursor));
        Ok(())
    }

    fn open_write(&mut self, session: &mut SessionInner, prefix: Bson) -> Result<()> {
        session.auto_start_transaction(TransactionType::Write)?;
        let mut cursor = self.kv_engine.open_multi_cursor(Some(session.kv_session()));
        cursor.go_to_min()?;
        self.r1 = Some(Cursor::new(prefix, cursor));
        Ok(())
    }

    fn reset_cursor(&mut self, is_empty: &Cell<bool>) -> Result<()> {
        let cursor = self.r1.as_mut().unwrap();
        cursor.reset()?;
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

    fn find_by_primary_key(&mut self) -> Result<bool> {
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

    fn next(&mut self) -> Result<()> {
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

    fn inc_numeric(key: &str, a: &Bson, b: &Bson) -> Result<Bson> {
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
                return Err(Error::CannotApplyOperation(Box::new(CannotApplyOperationForTypes {
                    op_name: "$inc".into(),
                    field_name: key.into(),
                    field_type: a.to_string(),
                    target_type: b.to_string(),
                })));
            }
        };
        Ok(val)
    }

    fn mul_numeric(key: &str, a: &Bson, b: &Bson) -> Result<Bson> {
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
                return Err(Error::CannotApplyOperation(Box::new(CannotApplyOperationForTypes {
                    op_name: "$mul".into(),
                    field_name: key.into(),
                    field_type: a.to_string(),
                    target_type: b.to_string(),
                })));
            }
        };
        Ok(val)
    }

    fn inc_field(&mut self, field_id: usize) -> Result<()> {
        let key = self.program.static_values[field_id].as_str().unwrap();

        let value_index = self.stack.len() - 1;
        let doc_index = self.stack.len() - 2;

        let value = self.stack[value_index].clone();

        let mut_doc = self.stack[doc_index].as_document_mut().unwrap();

        match mut_doc.get(key) {
            Some(Bson::Null) => {
                return Err(Error::IncrementNullField);
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

    fn mul_field(&mut self, field_id: usize) -> Result<()> {
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

    fn unset_field(&mut self, field_id: u32) -> Result<()> {
        let key = self.program.static_values[field_id as usize].as_str().unwrap();

        let doc_index = self.stack.len() - 1;
        let mut_doc = self.stack[doc_index].as_document_mut().unwrap();

        let _ = mut_doc.remove(key);

        Ok(())
    }

    fn array_size(&mut self) -> Result<usize> {
        let top = self.stack.len() - 1;
        let doc = crate::try_unwrap_array!("ArraySize", &self.stack[top]);
        Ok(doc.len())
    }

    fn array_push(&mut self) -> Result<()> {
        let st = self.stack.len();
        let val = self.stack[st - 1].clone();
        let mut array_value = match &self.stack[st - 2] {
            Bson::Array(arr) => arr.clone(),
            _ => {
                let name = format!("{}", self.stack[st-  2]);
                return Err(Error::UnexpectedTypeForOp(mk_unexpected_type_for_op(
                    "$push", "Array", name
                )))
            }
        };
        array_value.push(val);
        self.stack[st - 2] = array_value.into();
        Ok(())
    }

    fn array_pop_first(&mut self) -> Result<()> {
        let st = self.stack.len();
        let array_value = match &mut self.stack[st - 1] {
            Bson::Array(arr) => arr,
            _ => {
                let name = format!("{}", self.stack[st - 1]);
                return Err(Error::UnexpectedTypeForOp(mk_unexpected_type_for_op(
                    "$pop", "Array", name
                )))
            }
        };
        array_value.drain(0..1);

        Ok(())
    }

    fn array_pop_last(&mut self) -> Result<()> {
        let st = self.stack.len();
        let array_value = match &mut self.stack[st - 1] {
            Bson::Array(arr) => arr,
            _ => {
                let name = format!("{}", self.stack[st - 1]);
                return Err(Error::UnexpectedTypeForOp(mk_unexpected_type_for_op(
                    "$pop", "Array", name
                )))
            }
        };
        array_value.pop();

        Ok(())
    }

    pub(crate) fn execute(&mut self, session: &mut SessionInner) -> Result<()> {
        if self.state == VmState::Halt {
            return Err(Error::VmIsHalt);
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
                        let top = &self.stack[self.stack.len() - 1];
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
                        let doc_buf = bson::to_vec(doc)?;

                        let updated = {
                            let cursor = self.r1.as_mut().unwrap();
                            session.update_cursor_current(
                                cursor.multi_cursor_mut(),
                                &doc_buf,
                            )?
                        };
                        if updated {
                            self.r2 += 1;
                        }

                        self.pc = self.pc.add(1);
                    }

                    DbOp::DeleteCurrent => {
                        let deleted = {
                            let cursor = self.r1.as_mut().unwrap();
                            session.delete_cursor_current(cursor.multi_cursor_mut())?
                        };
                        if deleted {
                            self.r2 += 1;
                        }

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
                            let cmp_result = crate::utils::bson::value_cmp(top2, item);
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

                        try_vm!(self, self.open_read(session, prefix));

                        self.pc = self.pc.add(5);
                    }

                    DbOp::OpenWrite => {
                        let prefix_idx = self.pc.add(1).cast::<u32>().read();
                        let prefix = self.program.static_values[prefix_idx as usize].clone();

                        try_vm!(self, self.open_write(session, prefix));

                        self.pc = self.pc.add(5);
                    }

                    DbOp::ResultRow => {
                        self.pc = self.pc.add(1);
                        self.state = VmState::HasRow;
                        return Ok(());
                    }

                    DbOp::Close => {
                        self.r1 = None;
                        session.auto_commit()?;

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

}
