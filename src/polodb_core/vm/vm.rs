/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use crate::cursor::Cursor;
use crate::errors::{
    AllError, CannotApplyOperationForTypes, FieldTypeUnexpectedStruct, RegexError,
    UnexpectedTypeForOpStruct,
};
use crate::index::{IndexHelper, IndexHelperOperation};
use crate::session::SessionInner;
use crate::utils::bson::ElementType;
use crate::vm::op::DbOp;
use crate::vm::SubProgram;
use crate::{Error, LsmKv, Metrics, Result, TransactionType};
use bson::spec::ElementType as BsonElementType;
use bson::{Bson, Document};
use regex::RegexBuilder;
use std::cell::Cell;
use std::cmp::Ordering;

macro_rules! try_vm {
    ($self:ident, $action:expr) => {
        match $action {
            Ok(result) => result,
            Err(err) => {
                $self.state = VmState::Halt;
                return Err(err);
            }
        }
    };
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

struct VMFrame {
    stack_begin_pos: usize,
    return_pos: usize,
}

impl Default for VMFrame {
    fn default() -> Self {
        VMFrame {
            stack_begin_pos: 0,
            return_pos: usize::MAX,
        }
    }
}

pub(crate) struct VM {
    kv_engine: LsmKv,
    pub(crate) state: VmState,
    pc: *const u8,
    r0: i32, // usually the logic register
    r1: Option<Cursor>,
    pub(crate) r2: i64, // usually the counter
    r3: usize,
    stack: Vec<Bson>,
    frames: Vec<VMFrame>,
    pub(crate) program: SubProgram,
    global_vars: Vec<Bson>,
    metrics: Metrics,
}

fn generic_cmp(op: DbOp, val1: &Bson, val2: &Bson) -> Result<bool> {
    let ord = crate::utils::bson::value_cmp(val1, val2)?;
    let result = matches!(
        (op, ord),
        (DbOp::Equal, Ordering::Equal)
            | (DbOp::Greater, Ordering::Greater)
            | (DbOp::GreaterEqual, Ordering::Equal)
            | (DbOp::GreaterEqual, Ordering::Greater)
            | (DbOp::Less, Ordering::Less)
            | (DbOp::LessEqual, Ordering::Equal)
            | (DbOp::LessEqual, Ordering::Less)
    );
    Ok(result)
}

impl VM {
    pub(crate) fn new(kv_engine: LsmKv, program: SubProgram, metrics: Metrics) -> VM {
        let stack = Vec::with_capacity(STACK_SIZE);
        let pc = program.instructions.as_ptr();
        let mut global_vars = Vec::<Bson>::new();

        for item in &program.global_variables {
            global_vars.push(item.init_value.clone());
        }

        VM {
            kv_engine,
            state: VmState::Init,
            pc,
            r0: 0,
            r1: None,
            r2: 0,
            r3: 0,
            stack,
            frames: vec![VMFrame::default()],
            program,
            global_vars,
            metrics,
        }
    }

    fn prefix_bytes_from_bson(val: Bson) -> Result<Vec<u8>> {
        match val {
            Bson::String(_) => {
                let mut prefix_bytes = Vec::<u8>::new();
                crate::utils::bson::stacked_key_bytes(&mut prefix_bytes, &val)?;
                Ok(prefix_bytes)
            }

            Bson::Binary(bin) => Ok(bin.bytes),

            _ => panic!("unexpected bson value: {:?}", val),
        }
    }

    fn open_read(&mut self, session: &mut SessionInner, prefix: Bson) -> Result<()> {
        session.auto_start_transaction(TransactionType::Read)?;
        let mut cursor = self.kv_engine.open_multi_cursor(Some(session.kv_session()));
        cursor.go_to_min()?;

        let prefix_bytes = VM::prefix_bytes_from_bson(prefix)?;

        self.r1 = Some(Cursor::new(prefix_bytes, cursor));
        Ok(())
    }

    fn open_write(&mut self, session: &mut SessionInner, prefix: Bson) -> Result<()> {
        session.auto_start_transaction(TransactionType::Write)?;
        let mut cursor = self.kv_engine.open_multi_cursor(Some(session.kv_session()));
        cursor.go_to_min()?;

        let prefix_bytes = VM::prefix_bytes_from_bson(prefix)?;

        self.r1 = Some(Cursor::new(prefix_bytes, cursor));
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

    fn find_by_index(&mut self, session: &mut SessionInner) -> Result<bool> {
        let stack_len = self.stack.len();
        // let col_name = self.stack[stack_len - 1].as_str().expect("col_name must be string").to_string();
        let query_value = &self.stack[stack_len - 2];

        let cursor = self.r1.as_mut().unwrap();
        let result = cursor.reset_by_index_value(query_value)?;

        if !result {
            return Ok(false);
        }

        let key = cursor.peek_key().expect("key must exist");

        let index_value = self.read_index_value_by_index_key(key.as_ref(), session)?;

        if index_value.is_none() {
            return Ok(false);
        }

        self.stack.push(index_value.unwrap());

        self.metrics.add_find_by_index_count();

        Ok(true)
    }

    fn read_index_value_by_index_key(
        &mut self,
        index_key: &[u8],
        session: &mut SessionInner,
    ) -> Result<Option<Bson>> {
        let slices = crate::utils::bson::split_stacked_keys(index_key.as_ref())?;
        let pkey = slices.last().expect("pkey must exist");

        let col_name = &slices[1];

        let pkey_in_kv = crate::utils::bson::stacked_key(vec![col_name, pkey])?;

        let mut value_cursor = self.kv_engine.open_multi_cursor(Some(session.kv_session()));
        value_cursor.go_to_min()?;

        value_cursor.seek(pkey_in_kv.as_slice())?;

        let current_key = value_cursor.key();
        if current_key.is_none() {
            return Ok(None);
        }

        let current_key = current_key.unwrap();

        if current_key.as_ref().cmp(pkey_in_kv.as_slice()) != Ordering::Equal {
            return Ok(None);
        }

        let buf = value_cursor.value(self.kv_engine.inner.as_ref())?.unwrap();
        let doc = bson::from_slice(buf.as_ref())?;

        Ok(Some(Bson::Document(doc)))
    }

    fn next(&mut self) -> Result<()> {
        let cursor = self.r1.as_mut().unwrap();
        cursor.next()?;
        match cursor.peek_data(self.kv_engine.inner.as_ref())? {
            Some(bytes) => {
                let doc = bson::from_slice(bytes.as_ref())?;
                self.stack.push(Bson::Document(doc));

                debug_assert!(
                    self.stack.len() <= 64,
                    "stack too large: {}",
                    self.stack.len()
                );

                self.r0 = 1;
            }

            None => {
                self.r0 = 0;
            }
        }
        Ok(())
    }

    fn next_index_value(&mut self, session: &mut SessionInner) -> Result<()> {
        let cursor = self.r1.as_mut().unwrap();
        cursor.next()?;
        let current_key = cursor.peek_key();
        if current_key.is_none() {
            self.r0 = 0;
            return Ok(());
        }
        let current_key = current_key.unwrap();
        if !current_key.starts_with(cursor.prefix_bytes.as_slice()) {
            self.r0 = 0;
            return Ok(());
        }

        let value_opt = self.read_index_value_by_index_key(current_key.as_ref(), session)?;
        if value_opt.is_none() {
            self.r0 = 0;
            return Ok(());
        }

        self.stack.push(value_opt.unwrap());

        self.r0 = 1;

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
                return Err(CannotApplyOperationForTypes {
                    op_name: "$inc".into(),
                    field_name: key.into(),
                    field_type: a.to_string(),
                    target_type: b.to_string(),
                }
                .into());
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
                return Err(CannotApplyOperationForTypes {
                    op_name: "$mul".into(),
                    field_name: key.into(),
                    field_type: a.to_string(),
                    target_type: b.to_string(),
                }
                .into());
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
        let key = self.program.static_values[field_id as usize]
            .as_str()
            .unwrap();

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
                let name = format!("{}", self.stack[st - 2]);
                return Err(UnexpectedTypeForOpStruct {
                    operation: "$push",
                    expected_ty: "Array",
                    actual_ty: name,
                }
                .into());
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
                return Err(UnexpectedTypeForOpStruct {
                    operation: "$pop",
                    expected_ty: "Array",
                    actual_ty: name,
                }
                .into());
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
                return Err(UnexpectedTypeForOpStruct {
                    operation: "$pop",
                    expected_ty: "Array",
                    actual_ty: name,
                }
                .into());
            }
        };
        array_value.pop();

        Ok(())
    }

    fn update_current(&mut self, session: &mut SessionInner) -> Result<()> {
        let top_index = self.stack.len() - 1;
        let top_value = &self.stack[top_index];

        let doc = top_value.as_document().unwrap();
        let doc_buf = bson::to_vec(doc)?;

        let updated = {
            let cursor = self.r1.as_mut().unwrap();
            cursor.update_current(session, &doc_buf)?
        };

        if updated {
            self.r2 += 1;
        }

        Ok(())
    }

    fn insert_index(&mut self, index_info_id: u32, session: &mut SessionInner) -> Result<()> {
        let info = &self.program.index_infos[index_info_id as usize];

        let index_meta = &info.indexes;

        let data_doc = self.stack[self.stack.len() - 1].as_document().unwrap();
        let pkey = data_doc.get("_id").unwrap();

        for (index_name, index_info) in index_meta {
            IndexHelper::try_execute_with_index_info(
                IndexHelperOperation::Insert,
                data_doc,
                info.col_name.as_str(),
                pkey,
                index_name.as_str(),
                index_info,
                &self.kv_engine,
                session,
            )?;
        }

        Ok(())
    }

    fn delete_index(&mut self, index_info_id: u32, session: &mut SessionInner) -> Result<()> {
        let info = &self.program.index_infos[index_info_id as usize];

        let index_meta = &info.indexes;

        let data_doc = self.stack[self.stack.len() - 1].as_document().unwrap();
        let pkey = data_doc.get("_id").unwrap();

        for (index_name, index_info) in index_meta {
            IndexHelper::try_execute_with_index_info(
                IndexHelperOperation::Delete,
                data_doc,
                info.col_name.as_str(),
                pkey,
                index_name.as_str(),
                index_info,
                &self.kv_engine,
                session,
            )?;
        }

        Ok(())
    }

    fn ret(&mut self, return_size: usize) {
        let frame = self.frames.pop().unwrap();

        let clone_start_pos = self.stack.len() - return_size;
        for i in 0..return_size {
            self.stack[frame.stack_begin_pos + i] = self.stack[clone_start_pos + i].clone();
        }

        self.stack
            .resize(frame.stack_begin_pos + return_size, Bson::Null);

        self.reset_location(frame.return_pos as u32);
    }

    fn inc(&mut self) {
        match self.stack.last_mut() {
            Some(Bson::Int32(n)) => {
                *n += 1;
            }
            Some(Bson::Int64(n)) => {
                *n += 1;
            }
            Some(Bson::Double(d)) => {
                *d += 1.0;
            }
            _ => (),
        }
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

                    DbOp::Inc => {
                        self.inc();
                        self.pc = self.pc.add(1);
                    }

                    DbOp::IncR2 => {
                        self.r2 += 1;
                        self.pc = self.pc.add(1);
                    }

                    DbOp::IfTrue => {
                        let location = self.pc.add(1).cast::<u32>().read();
                        if self.r0 != 0 {
                            // true
                            self.reset_location(location);
                        } else {
                            self.pc = self.pc.add(5);
                        }
                    }

                    DbOp::IfFalse => {
                        let location = self.pc.add(1).cast::<u32>().read();
                        if self.r0 == 0 {
                            // false
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

                    DbOp::FindByIndex => {
                        let location = self.pc.add(1).cast::<u32>().read();

                        let found = try_vm!(self, self.find_by_index(session));

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

                    DbOp::NextIndexValue => {
                        try_vm!(self, self.next_index_value(session));
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

                    DbOp::PushTrue => {
                        self.stack.push(Bson::Boolean(true));
                        self.pc = self.pc.add(1);
                    }

                    DbOp::PushFalse => {
                        self.stack.push(Bson::Boolean(false));
                        self.pc = self.pc.add(1);
                    }

                    DbOp::PushDocument => {
                        self.stack.push(Bson::Document(Document::new()));
                        self.pc = self.pc.add(1);
                    }

                    DbOp::PushR0 => {
                        self.stack.push(Bson::from(self.r0));
                        self.pc = self.pc.add(1);
                    }

                    DbOp::StoreR0 => {
                        let top = self.stack_top();
                        self.r0 = match top {
                            Bson::Int32(i) => *i,
                            Bson::Int64(i) => *i as i32,
                            Bson::Boolean(bl) => {
                                if *bl {
                                    1
                                } else {
                                    0
                                }
                            }
                            _ => panic!("store r0 failed"),
                        };
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
                                let err = FieldTypeUnexpectedStruct {
                                    field_name: key_name.into(),
                                    expected_ty: "Document".into(),
                                    actual_ty: name,
                                };
                                self.state = VmState::Halt;
                                return Err(err.into());
                            }
                        };

                        match crate::utils::bson::try_get_document_value(doc, key_name) {
                            Some(val) => {
                                self.r0 = 1;
                                self.stack.push(val);
                                self.pc = self.pc.add(9);
                            }

                            None => {
                                self.r0 = 0;
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

                        let key = self.program.static_values[filed_id as usize]
                            .as_str()
                            .unwrap();

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
                        try_vm!(self, self.update_current(session));

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

                    DbOp::InsertIndex => {
                        let index_info_id = self.pc.add(1).cast::<u32>().read();

                        self.insert_index(index_info_id, session)?;

                        self.pc = self.pc.add(5);
                    }

                    DbOp::DeleteIndex => {
                        let index_info_id = self.pc.add(1).cast::<u32>().read();

                        self.delete_index(index_info_id, session)?;

                        self.pc = self.pc.add(5);
                    }

                    DbOp::Dup => {
                        self.stack.push(self.stack.last().unwrap().clone());
                        self.pc = self.pc.add(1);
                    }

                    DbOp::Pop => {
                        self.stack.pop();
                        self.pc = self.pc.add(1);
                    }

                    DbOp::Pop2 => {
                        let offset = self.pc.add(1).cast::<u32>().read();

                        self.stack
                            .resize(self.stack.len() - (offset as usize), Bson::Null);

                        self.pc = self.pc.add(5);
                    }

                    DbOp::Equal
                    | DbOp::Greater
                    | DbOp::GreaterEqual
                    | DbOp::Less
                    | DbOp::LessEqual => {
                        let val1 = &self.stack[self.stack.len() - 2];
                        let val2 = &self.stack[self.stack.len() - 1];

                        let cmp = try_vm!(self, generic_cmp(op, val1, val2));

                        self.r0 = if cmp { 1 } else { 0 };

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
                            match top2.element_type() {
                                BsonElementType::Array => {
                                    // ? Is the conversion costly to run each time ?
                                    // ? Or should be convert the array before the loop ?
                                    // * DB value should contain at least one element from input array
                                    if top2.as_array().unwrap().contains(item) {
                                        self.r0 = 1;
                                        break;
                                    }
                                }
                                _ => {
                                    let cmp_result = crate::utils::bson::value_cmp(top2, item);
                                    if let Ok(Ordering::Equal) = cmp_result {
                                        self.r0 = 1;
                                        break;
                                    }
                                }
                            }
                        }

                        self.pc = self.pc.add(1);
                    }

                    DbOp::Regex => {
                        let val1 = &self.stack[self.stack.len() - 2];
                        let val2 = &self.stack[self.stack.len() - 1];

                        self.r0 = 0;

                        if let Bson::RegularExpression(re) = val2 {
                            let mut re_build = RegexBuilder::new(re.pattern.as_str());
                            for char in re.options.chars() {
                                match char {
                                    'i' => {
                                        re_build.case_insensitive(true);
                                    }
                                    'm' => {
                                        re_build.multi_line(true);
                                    }
                                    's' => {
                                        re_build.dot_matches_new_line(true);
                                    }
                                    'u' => {
                                        re_build.unicode(true);
                                    }
                                    'U' => {
                                        re_build.swap_greed(true);
                                    }
                                    'x' => {
                                        re_build.ignore_whitespace(true);
                                    }
                                    _ => {
                                        return Err(Error::from(RegexError {
                                            error: format!("unknown regex option: {}", char),
                                            expression: re.pattern.clone(),
                                            options: re.options.clone(),
                                        }));
                                    }
                                }
                            }

                            let re_build = re_build.build().map_err(|err| {
                                Error::from(RegexError {
                                    error: format!("regex build error: {err}"),
                                    expression: re.pattern.clone(),
                                    options: re.options.clone(),
                                })
                            })?;

                            if re_build.is_match(&val1.to_string()) {
                                self.r0 = 1;
                            }
                        }

                        self.pc = self.pc.add(1);
                    }

                    DbOp::Not => {
                        self.r0 = if self.r0 == 0 { 1 } else { 0 };

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

                    DbOp::Call => {
                        let location = self.pc.add(1).cast::<u32>().read();
                        let size_of_param = self.pc.add(5).cast::<u32>().read() as usize;

                        let start = self.program.instructions.as_ptr() as usize;
                        let return_pos = self.pc.add(9).sub(start) as usize;

                        self.frames.push(VMFrame {
                            stack_begin_pos: self.stack.len() - size_of_param,
                            return_pos,
                        });

                        self.reset_location(location);
                    }

                    DbOp::Ret0 => {
                        self.ret(0);
                    }

                    DbOp::Ret => {
                        let return_size = self.pc.add(1).cast::<u32>().read() as usize;
                        self.ret(return_size);
                    }

                    DbOp::IfFalseRet => {
                        let return_size = self.pc.add(1).cast::<u32>().read() as usize;
                        if self.r0 == 0 {
                            // false
                            self.ret(return_size);
                        } else {
                            self.pc = self.pc.add(5);
                        }
                    }

                    DbOp::LoadGlobal => {
                        let idx = self.pc.add(1).cast::<u32>().read();
                        self.stack.push(self.global_vars[idx as usize].clone());
                        self.pc = self.pc.add(5);
                    }

                    DbOp::StoreGlobal => {
                        let idx = self.pc.add(1).cast::<u32>().read();
                        self.global_vars[idx as usize] = self.stack.last().unwrap().clone();
                        self.pc = self.pc.add(5);
                    }

                    DbOp::_EOF | DbOp::Halt => {
                        self.r1 = None;
                        self.state = VmState::Halt;
                        return Ok(());
                    }

                    DbOp::All => {
                        let cmp_arr = &self.stack[self.stack.len() - 1];
                        let db_arr =
                            &self.stack[self.stack.len() - 2]
                                .as_array()
                                .ok_or(Error::from(AllError {
                                    field_key: String::new(), // todo: use key field
                                    field_type: ElementType::from(
                                        self.stack[self.stack.len() - 2].element_type(),
                                    )
                                    .to_string(),
                                    field_value: self.stack[self.stack.len() - 2].to_string(),
                                }))?;

                        self.r0 = 0;

                        let mut found_all = true;
                        for item in cmp_arr.as_array().unwrap().iter() {
                            // * all element must be in db_arr unlike $in
                            if !db_arr.contains(item) {
                                found_all = false;
                                break;
                            }
                        }

                        if found_all {
                            self.r0 = 1;
                        }

                        self.pc = self.pc.add(1);
                    }
                }
            }
        }
    }
}
