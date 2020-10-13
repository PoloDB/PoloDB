use std::rc::Rc;
use std::collections::HashMap;
use lazy_static::lazy_static;
use polodb_bson::{Value, Document};
use crate::vm::SubProgram;
use crate::vm::op::DbOp;
use crate::{DbResult, DbErr};
use crate::error::mk_field_name_type_unexpected;

type Callback = fn(&mut Codegen, &Value) -> DbResult<()>;

lazy_static! {
    static ref UPDATE_OP_MAP: HashMap<&'static str, Callback> = {
        let mut m: HashMap<&'static str, Callback> = HashMap::new();
        m.insert("$inc", update_op_inc);
        m.insert("$set", update_op_set);
        m.insert("$max", update_op_max);
        m.insert("$min", update_op_min);
        m.insert("$mul", update_op_mul);
        m.insert("$rename", update_op_rename);
        m
    };
}

fn update_op_inc(codegen: &mut Codegen, doc: &Value) -> DbResult<()> {
    let doc = match doc {
        Value::Document(doc) => doc,
        t => {
            let err = mk_field_name_type_unexpected("$inc", "Document".into(), t.ty_name());
            return Err(err);
        },
    };

    codegen.iterate_add_op(DbOp::IncField, doc.as_ref());

    Ok(())
}

fn update_op_set(codegen: &mut Codegen, doc: &Value) -> DbResult<()> {
    let doc = match doc {
        Value::Document(doc) => doc,
        t => {
            let err = mk_field_name_type_unexpected("$set", "Document".into(), t.ty_name());
            return Err(err);
        },
    };

    codegen.iterate_add_op(DbOp::SetField, doc.as_ref());

    Ok(())
}

fn update_op_max(_codegen: &mut Codegen, _doc: &Value) -> DbResult<()> {
    unimplemented!()
}

fn update_op_min(_codegen: &mut Codegen, _doc: &Value) -> DbResult<()> {
    unimplemented!()
}

fn update_op_mul(_codegen: &mut Codegen, _doc: &Value) -> DbResult<()> {
    unimplemented!()
}

fn update_op_rename(codegen: &mut Codegen, _doc: &Value) -> DbResult<()> {
    unimplemented!()
}

pub(super) struct Codegen {
    program: Box<SubProgram>,
}

impl Codegen {

    pub(super) fn new() -> Codegen {
        Codegen {
            program: Box::new(SubProgram::new())
        }
    }

    pub(super) fn take(self) -> SubProgram {
        *self.program
    }

    pub(super) fn add_query_layout<F>(&mut self, query: &Document, result_callback: F) -> DbResult<()> where
        F: FnOnce(&mut Codegen) -> DbResult<()> {
        let next_preserve_location = self.current_location();
        self.add_next(0);

        self.add(DbOp::Close);
        self.add(DbOp::Halt);

        let not_found_branch_preserve_location = self.current_location();
        self.add(DbOp::Pop);
        self.add(DbOp::Pop);
        self.add(DbOp::Pop);  // pop the current value;
        self.add_goto(next_preserve_location);

        let get_field_failed_location = self.current_location();
        self.add(DbOp::Pop);
        self.add_goto(next_preserve_location);

        let compare_location: u32 = self.current_location();

        for (key, value) in query.iter() {
            let key_static_id = self.push_static(Value::String(Rc::new(key.clone())));
            let value_static_id = self.push_static(value.clone());

            self.add_get_field(key_static_id, get_field_failed_location);  // push a value1
            self.add_push_value(value_static_id);  // push a value2

            self.add(DbOp::Equal);
            // if not equal，go to next
            self.add_false_jump(not_found_branch_preserve_location);

            self.add(DbOp::Pop); // pop a value2
            self.add(DbOp::Pop); // pop a value1
        }

        self.update_next_location(next_preserve_location as usize, compare_location);

        result_callback(self)?;

        self.add_goto(next_preserve_location);

        Ok(())
    }

    pub(super) fn add_update_operation(&mut self, update: &Document) -> DbResult<()> {
        for (key, value) in update.iter() {
            let result = UPDATE_OP_MAP.get(key.as_str());
            match result {
                Some(callback) => {
                    callback(self, value)?;
                }

                None => {
                    return Err(DbErr::UnknownUpdateOperation(key.clone()))
                }
            }
        }
        Ok(())
    }

    fn iterate_add_op(&mut self, op: DbOp, doc: &Document) {
        for (key, value) in doc.iter() {
            let value_id = self.push_static(value.clone());
            self.add_push_value(value_id);

            let key_id = self.push_static(Value::String(Rc::new(key.into())));
            self.add_5bytes(op, key_id);

            self.add(DbOp::Pop);
        }
    }

    fn add_5bytes(&mut self, op: DbOp, op1: u32) {
        self.add(op);
        let bytes = op1.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    pub(super) fn add_open_read(&mut self, root_pid: u32) {
        self.add(DbOp::OpenRead);
        let bytes = root_pid.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    pub(super) fn add_open_write(&mut self, root_pid: u32) {
        self.add(DbOp::OpenWrite);
        let bytes = root_pid.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    #[inline]
    pub(super) fn add(&mut self, op: DbOp) {
        self.program.instructions.push(op as u8);
    }

    #[inline]
    pub(super) fn current_location(&self) -> u32 {
        self.program.instructions.len() as u32
    }

    pub(super) fn push_static(&mut self, value: Value) -> u32 {
        let pos = self.program.static_values.len() as u32;
        self.program.static_values.push(value);
        pos
    }

    pub(super) fn add_get_field(&mut self, static_id: u32, failed_location: u32) {
        self.add(DbOp::GetField);
        let bytes = static_id.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
        let bytes = failed_location.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    pub(super) fn add_push_value(&mut self, static_id: u32) {
        self.add(DbOp::PushValue);
        let bytes = static_id.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    pub(super) fn add_false_jump(&mut self, location: u32) {
        self.add(DbOp::FalseJump);
        let bytes = location.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    #[inline]
    pub(super) fn update_next_location(&mut self, pos: usize, location: u32) {
        let loc_be = location.to_le_bytes();
        self.program.instructions[pos + 1..pos + 5].copy_from_slice(&loc_be);
    }

    pub(super) fn add_goto(&mut self, location: u32) {
        self.add(DbOp::Goto);
        let bytes = location.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    pub(super) fn add_next(&mut self, location: u32) {
        self.add(DbOp::Next);
        let bytes = location.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

}
