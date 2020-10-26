use std::rc::Rc;
use polodb_bson::{Value, Document};
use crate::vm::SubProgram;
use crate::vm::op::DbOp;
use crate::{DbResult, DbErr};
use crate::error::mk_field_name_type_unexpected;

macro_rules! try_unwrap_document {
    ($op_name:tt, $doc:expr) => {
        match $doc {
            Value::Document(doc) => doc,
            t => {
                let err = mk_field_name_type_unexpected($op_name, "Document".into(), t.ty_name());
                return Err(err);
            },
        }
    };
}

mod update_op {
    use polodb_bson::Value;
    use std::rc::Rc;
    use crate::vm::codegen::Codegen;
    use crate::DbResult;
    use crate::vm::op::DbOp;
    use crate::error::mk_field_name_type_unexpected;

    pub(super) fn update_op_min_max(codegen: &mut Codegen, doc: &Value, min: bool) -> DbResult<()> {
        let doc = try_unwrap_document!("$min", doc);

        for (key, value) in doc.iter() {
            let rc_str: Rc<String> = Rc::new(key.into());
            let key_id_1 = codegen.push_static(Value::String(rc_str.clone()));
            let key_id_2 = codegen.push_static(Value::String(rc_str));
            let value_id = codegen.push_static(value.clone());

            let begin_loc = codegen.current_location();
            codegen.add_get_field(key_id_1, 0);  // stack +1

            codegen.add_push_value(value_id);  // stack +2

            codegen.add(DbOp::Cmp);

            let jmp_loc = codegen.current_location();
            if min {
                codegen.add_5bytes(DbOp::IfLess, 0);
            } else {
                codegen.add_5bytes(DbOp::IfGreater, 0);
            }

            let goto_loc = codegen.current_location();
            codegen.add_goto(0);

            let loc = codegen.current_location();
            codegen.update_next_location(jmp_loc as usize, loc);

            codegen.add(DbOp::Pop);
            codegen.add(DbOp::Pop);  // stack

            codegen.add_push_value(value_id);

            codegen.add_5bytes(DbOp::SetField, key_id_2);

            codegen.add(DbOp::Pop);

            let goto_next_loc = codegen.current_location();
            codegen.add_goto(0);

            let loc = codegen.current_location();
            codegen.update_next_location(goto_loc as usize, loc);

            codegen.add(DbOp::Pop);
            codegen.add(DbOp::Pop);

            let loc = codegen.current_location();
            codegen.update_next_location(goto_next_loc as usize, loc);
            codegen.update_failed_location(begin_loc as usize, loc);
        }

        Ok(())
    }

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

    fn add_query_layout_has_pkey<F>(&mut self, pkey: Value, query: &Document, result_callback: F) -> DbResult<()> where
        F: FnOnce(&mut Codegen) -> DbResult<()> {

        let pkey_id = self.push_static(pkey);
        self.add_push_value(pkey_id);

        let reset_location = self.current_location();
        self.add_5bytes(DbOp::FindByPrimaryKey, 0);

        let goto_loc = self.current_location();
        self.add_goto(0);

        let close_location = self.current_location();
        self.add(DbOp::Pop);
        self.add(DbOp::Close);
        self.add(DbOp::Halt);

        self.update_next_location(reset_location as usize, close_location);

        let result_location = self.current_location();
        for (key, value) in query.iter() {
            if key == "_id" {
                continue;
            }

            let key_static_id = self.push_static(Value::String(Rc::new(key.clone())));
            let value_static_id = self.push_static(value.clone());

            self.add_get_field(key_static_id, 0);  // push a value1
            self.add_push_value(value_static_id);  // push a value2

            self.add(DbOp::Equal);
            // if not equal，go to next
            self.add_false_jump(close_location);

            self.add(DbOp::Pop); // pop a value2
            self.add(DbOp::Pop); // pop a value1
        }

        result_callback(self)?;

        self.add_goto(close_location);

        self.update_next_location(goto_loc as usize, result_location);

        Ok(())
    }

    pub(super) fn add_query_layout<F>(&mut self, query: &Document, result_callback: F) -> DbResult<()> where
        F: FnOnce(&mut Codegen) -> DbResult<()> {

        if let Some(id_value) = query.pkey_id() {
            return self.add_query_layout_has_pkey(id_value, query, result_callback);
        }

        let rewind_location = self.current_location();
        self.add_5bytes(DbOp::Rewind, 0);

        let goto_compare_loc = self.current_location();
        self.add_goto(0);

        let next_preserve_location = self.current_location();
        self.add_next(0);

        let close_location = self.current_location();
        self.update_next_location(rewind_location as usize, close_location);

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
            self.add_query_tuple(
                key, value,
                get_field_failed_location,
                not_found_branch_preserve_location
            )?;
        }

        self.update_next_location(next_preserve_location as usize, compare_location);
        self.update_next_location(goto_compare_loc as usize, compare_location);

        result_callback(self)?;

        self.add_goto(next_preserve_location);

        Ok(())
    }

    fn add_query_tuple(&mut self, key: &str, value: &Value, get_field_failed_location: u32, not_found_branch: u32) -> DbResult<()> {
        if key.chars().nth(0).unwrap() == '$' {
            match key {
                "$and" => {
                    unimplemented!()
                }

                "$or" => {
                    unimplemented!()
                }

                _ => {
                    return Err(DbErr::NotAValidField(key.into()));
                }
            }
        } else {
            match value {
                Value::Document(doc) => {
                    return self.add_query_tuple_document(
                        key, doc.as_ref(),
                        get_field_failed_location, not_found_branch
                    );
                }

                Value::Array(_) => {
                    return Err(DbErr::NotAValidField(key.into()));
                }

                _ => {
                    let key_static_id = self.push_static(key.into());
                    self.add_get_field(key_static_id, get_field_failed_location);  // push a value1

                    let value_static_id = self.push_static(value.clone());
                    self.add_push_value(value_static_id);  // push a value2

                    self.add(DbOp::Equal);
                    // if not equal，go to next
                    self.add_false_jump(not_found_branch);

                    self.add(DbOp::Pop); // pop a value2
                    self.add(DbOp::Pop); // pop a value1
                }
            }
        }
        Ok(())
    }

    // very complex query document
    fn add_query_tuple_document(&mut self, key: &str, value: &Document, get_field_failed_location: u32, not_found_branch: u32) -> DbResult<()> {
        for (sub_key, sub_value) in value.iter() {
            match sub_key.as_str() {
                "$eq" => {
                    let key_static_id = self.push_static(key.into());
                    self.add_get_field(key_static_id, get_field_failed_location);  // push a value1

                    let stat_val_id = self.push_static(sub_value.clone().into());
                    self.add_push_value(stat_val_id);
                    self.add(DbOp::Equal);

                    // if not equal，go to next
                    self.add_false_jump(not_found_branch);

                    self.add(DbOp::Pop); // pop a value2
                    self.add(DbOp::Pop); // pop a value1
                }

                "$gt" => {
                    let key_static_id = self.push_static(key.into());
                    self.add_get_field(key_static_id, get_field_failed_location);  // push a value1

                    let stat_val_id = self.push_static(sub_value.clone().into());
                    self.add_push_value(stat_val_id);
                    self.add(DbOp::Cmp);

                    // equal, r0 == 0
                    self.add_false_jump(not_found_branch);
                    // less
                    self.add_less_jump(not_found_branch);

                    self.add(DbOp::Pop); // pop a value2
                    self.add(DbOp::Pop); // pop a value1
                }

                "$gte" => {
                    let key_static_id = self.push_static(key.into());
                    self.add_get_field(key_static_id, get_field_failed_location);  // push a value1

                    let stat_val_id = self.push_static(sub_value.clone().into());
                    self.add_push_value(stat_val_id);
                    self.add(DbOp::Cmp);

                    self.add_less_jump(not_found_branch);

                    self.add(DbOp::Pop); // pop a value2
                    self.add(DbOp::Pop); // pop a value1
                }

                // check the value is array
                "$in" => {
                    match sub_value {
                        Value::Array(_) => (),
                        _ => {
                            return Err(DbErr::NotAValidField(key.into()));
                        }
                    }

                    let key_static_id = self.push_static(key.into());
                    self.add_get_field(key_static_id, get_field_failed_location);  // push a value1

                    let stat_val_id = self.push_static(sub_value.clone().into());
                    self.add_push_value(stat_val_id);
                    self.add(DbOp::In);

                    self.add_false_jump(not_found_branch);

                    self.add(DbOp::Pop); // pop a value2
                    self.add(DbOp::Pop); // pop a value1
                }

                "$lt" => {
                    let key_static_id = self.push_static(key.into());
                    self.add_get_field(key_static_id, get_field_failed_location);  // push a value1

                    let stat_val_id = self.push_static(sub_value.clone().into());
                    self.add_push_value(stat_val_id);
                    self.add(DbOp::Cmp);

                    // equal, r0 == 0
                    self.add_false_jump(not_found_branch);
                    // less
                    self.add_greater_jump(not_found_branch);

                    self.add(DbOp::Pop); // pop a value2
                    self.add(DbOp::Pop); // pop a value1
                }

                "$lte" => {
                    let key_static_id = self.push_static(key.into());
                    self.add_get_field(key_static_id, get_field_failed_location);  // push a value1

                    let stat_val_id = self.push_static(sub_value.clone().into());
                    self.add_push_value(stat_val_id);
                    self.add(DbOp::Cmp);

                    // less
                    self.add_greater_jump(not_found_branch);

                    self.add(DbOp::Pop); // pop a value2
                    self.add(DbOp::Pop); // pop a value1
                }

                "$ne" => {
                    let key_static_id = self.push_static(key.into());
                    self.add_get_field(key_static_id, get_field_failed_location);  // push a value1

                    let stat_val_id = self.push_static(sub_value.clone().into());
                    self.add_push_value(stat_val_id);
                    self.add(DbOp::Equal);

                    // if equal，go to next
                    self.add_true_jump(not_found_branch);

                    self.add(DbOp::Pop); // pop a value2
                    self.add(DbOp::Pop); // pop a value1
                }

                "$nin" => {
                    match sub_value {
                        Value::Array(_) => (),
                        _ => {
                            return Err(DbErr::NotAValidField(key.into()));
                        }
                    }

                    let key_static_id = self.push_static(key.into());
                    self.add_get_field(key_static_id, get_field_failed_location);  // push a value1

                    let stat_val_id = self.push_static(sub_value.clone().into());
                    self.add_push_value(stat_val_id);
                    self.add(DbOp::In);

                    self.add_true_jump(not_found_branch);

                    self.add(DbOp::Pop); // pop a value2
                    self.add(DbOp::Pop); // pop a value1
                }

                _ => {
                    return Err(DbErr::NotAValidField(sub_key.into()));
                }
            }
        }
        Ok(())
    }

    pub(super) fn add_update_operation(&mut self, update: &Document) -> DbResult<()> {
        for (key, value) in update.iter() {
            match key.as_str() {
                "$inc" => {
                    let doc = try_unwrap_document!("$inc", value);

                    self.iterate_add_op(DbOp::IncField, doc.as_ref());
                }

                "$set" => {
                    let doc = try_unwrap_document!("$set", value);

                    self.iterate_add_op(DbOp::SetField, doc.as_ref());
                }

                "$max" => {
                    update_op::update_op_min_max(self, value, false)?;
                }

                "$min" => {
                    update_op::update_op_min_max(self, value, true)?;
                }

                "$mul" => {
                    let doc = try_unwrap_document!("$mul", value);

                    self.iterate_add_op(DbOp::MulField, doc.as_ref());
                }

                "$rename" => {
                    let doc = try_unwrap_document!("$set", value);

                    for (key, value) in doc.iter() {
                        let new_name = match value {
                            Value::String(new_name) => new_name.as_str(),
                            t => {
                                let err = mk_field_name_type_unexpected(key, "String".into(), t.ty_name());
                                return Err(err);
                            }
                        };

                        self.add_rename_field(key.as_str(), new_name);
                    }
                }

                "$unset" => {
                    let doc = try_unwrap_document!("$unset", value);

                    for (key, _) in doc.iter() {
                        self.add_unset_field(key.as_str());
                    }
                }

                _ => {
                    return Err(DbErr::UnknownUpdateOperation(key.clone()))
                }

            }
        }

        self.add(DbOp::UpdateCurrent);

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

    pub(super) fn add_5bytes(&mut self, op: DbOp, op1: u32) {
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

    #[inline]
    pub(super) fn add_false_jump(&mut self, location: u32) {
        self.add(DbOp::IfFalse);
        let bytes = location.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    #[inline]
    pub(super) fn add_true_jump(&mut self, location: u32) {
        self.add(DbOp::IfTrue);
        let bytes = location.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    #[inline]
    pub(super) fn add_less_jump(&mut self, location: u32) {
        self.add(DbOp::IfLess);
        let bytes = location.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    #[inline]
    pub(super) fn add_greater_jump(&mut self, location: u32) {
        self.add(DbOp::IfGreater);
        let bytes = location.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    pub(super) fn add_rename_field(&mut self, old_name: &str, new_name: &str) {
        let old_name_id = self.push_static(Value::String(Rc::new(old_name.into())));
        let new_name_id = self.push_static(Value::String(Rc::new(new_name.into())));
        let field_location = self.current_location();
        self.add_get_field(old_name_id, 0);
        self.add_5bytes(DbOp::SetField, new_name_id);
        self.add(DbOp::Pop);
        self.add_5bytes(DbOp::UnsetField, old_name_id);
        let current_loc = self.current_location();
        self.update_failed_location(field_location as usize, current_loc);
    }

    #[inline]
    pub(super) fn add_unset_field(&mut self, name: &str) {
        let value_id = self.push_static(Value::String(Rc::new(name.into())));
        self.add_5bytes(DbOp::UnsetField, value_id);
    }

    #[inline]
    pub(super) fn update_next_location(&mut self, pos: usize, location: u32) {
        let loc_be = location.to_le_bytes();
        self.program.instructions[pos + 1..pos + 5].copy_from_slice(&loc_be);
    }

    #[inline]
    pub(super) fn update_failed_location(&mut self, pos: usize, location: u32) {
        let loc_be = location.to_le_bytes();
        self.program.instructions[pos + 5..pos + 9].copy_from_slice(&loc_be);
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
