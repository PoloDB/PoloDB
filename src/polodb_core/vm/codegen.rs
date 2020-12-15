use polodb_bson::{Value, Document, Array};
use super::optimization::inverse_doc;
use crate::vm::SubProgram;
use crate::vm::op::DbOp;
use crate::{DbResult, DbErr};
use crate::error::mk_field_name_type_unexpected;

mod update_op {
    use polodb_bson::Value;
    use std::rc::Rc;
    use crate::vm::codegen::Codegen;
    use crate::DbResult;
    use crate::vm::op::DbOp;
    use crate::error::mk_field_name_type_unexpected;

    pub(super) fn update_op_min_max(codegen: &mut Codegen, doc: &Value, min: bool) -> DbResult<()> {
        let doc = crate::try_unwrap_document!("$min", doc);

        for (key, value) in doc.iter() {
            let rc_str: Rc<str> = key.as_str().into();
            let key_id_1 = codegen.push_static(Value::String(rc_str.clone()));
            let key_id_2 = codegen.push_static(Value::String(rc_str));
            let value_id = codegen.push_static(value.clone());

            let begin_loc = codegen.current_location();
            codegen.emit_get_field(key_id_1, 0);  // stack +1

            codegen.emit_push_value(value_id);  // stack +2

            codegen.emit(DbOp::Cmp);

            let jmp_loc = codegen.current_location();
            if min {
                codegen.emit(DbOp::IfLess);
                codegen.emit_u32(0);
            } else {
                codegen.emit(DbOp::IfGreater);
                codegen.emit_u32(0);
            }

            let goto_loc = codegen.current_location();
            codegen.emit_goto(0);

            let loc = codegen.current_location();
            codegen.update_next_location(jmp_loc as usize, loc);

            codegen.emit(DbOp::Pop);
            codegen.emit(DbOp::Pop);  // stack

            codegen.emit_push_value(value_id);

            codegen.emit(DbOp::SetField);
            codegen.emit_u32(key_id_2);

            codegen.emit(DbOp::Pop);

            let goto_next_loc = codegen.current_location();
            codegen.emit_goto(0);

            let loc = codegen.current_location();
            codegen.update_next_location(goto_loc as usize, loc);

            codegen.emit(DbOp::Pop);
            codegen.emit(DbOp::Pop);

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

    fn emit_query_layout_has_pkey<F>(&mut self, pkey: Value, query: &Document, result_callback: F) -> DbResult<()> where
        F: FnOnce(&mut Codegen) -> DbResult<()> {

        let pkey_id = self.push_static(pkey);
        self.emit_push_value(pkey_id);

        let reset_location = self.current_location();
        self.emit(DbOp::FindByPrimaryKey);
        self.emit_u32(0);

        let goto_loc = self.current_location();
        self.emit_goto(0);

        let close_location = self.current_location();
        self.emit(DbOp::Pop);
        self.emit(DbOp::Close);
        self.emit(DbOp::Halt);

        self.update_next_location(reset_location as usize, close_location);

        let result_location = self.current_location();
        for (key, value) in query.iter() {
            if key == "_id" {
                continue;
            }

            let key_static_id = self.push_static(Value::String(key.as_str().into()));
            let value_static_id = self.push_static(value.clone());

            self.emit_get_field(key_static_id, 0);  // push a value1
            self.emit_push_value(value_static_id);  // push a value2

            self.emit(DbOp::Equal);
            // if not equal，go to next
            self.emit_false_jump(close_location);

            self.emit(DbOp::Pop); // pop a value2
            self.emit(DbOp::Pop); // pop a value1
        }

        result_callback(self)?;

        self.emit_goto(close_location);

        self.update_next_location(goto_loc as usize, result_location);

        Ok(())
    }

    pub(super) fn emit_query_layout<F>(&mut self, query: &Document, result_callback: F) -> DbResult<()> where
        F: FnOnce(&mut Codegen) -> DbResult<()> {

        if let Some(id_value) = query.pkey_id() {
            if id_value.is_valid_key_type() {
                return self.emit_query_layout_has_pkey(id_value, query, result_callback);
            }
        }

        let rewind_location = self.current_location();
        self.emit(DbOp::Rewind);
        self.emit_u32(0);

        let goto_compare_loc = self.current_location();
        self.emit_goto(0);

        let next_preserve_location = self.current_location();
        self.emit_next(0);

        let close_location = self.current_location();
        self.update_next_location(rewind_location as usize, close_location);

        self.emit(DbOp::Close);
        self.emit(DbOp::Halt);

        let not_found_branch_preserve_location = self.current_location();
        self.emit(DbOp::RecoverStackPos);
        self.emit(DbOp::Pop);  // pop the current value;
        self.emit_goto(next_preserve_location);

        let get_field_failed_location = self.current_location();
        self.emit(DbOp::RecoverStackPos);
        self.emit(DbOp::Pop);
        self.emit_goto(next_preserve_location);

        // the top of the stack is the target document
        //
        // begin to execute compare logic
        // save the stack first
        let compare_location: u32 = self.current_location();
        self.emit(DbOp::SaveStackPos);

        for (key, value) in query.iter() {
            self.emit_query_tuple(
                key, value,
                get_field_failed_location,
                not_found_branch_preserve_location
            )?;
        }

        self.update_next_location(next_preserve_location as usize, compare_location);
        self.update_next_location(goto_compare_loc as usize, compare_location);

        result_callback(self)?;

        self.emit_goto(next_preserve_location);

        Ok(())
    }

    fn emit_logic_and(&mut self, arr: &Array, get_field_failed_location: u32, not_found_branch: u32) -> DbResult<()> {
        for item_doc_value in arr.iter() {
            let item_doc = crate::try_unwrap_document!("$and", item_doc_value);
            for (key, value) in item_doc.iter() {
                self.emit_query_tuple(key, value, get_field_failed_location, not_found_branch)?;
            }
        }

        Ok(())
    }

    fn emit_logic_or(&mut self, _arr: &Array, get_field_failed_location: u32, not_found_branch: u32) -> DbResult<()> {
        unimplemented!()
    }

    // case1: "$and" | "$or" -> [ Document ]
    // case2: "$not" -> Document
    // case3: "_id" -> Document
    fn emit_query_tuple(&mut self,
                        key: &str,
                        value: &Value,
                        get_field_failed_location: u32,
                        not_found_branch: u32) -> DbResult<()> {
        if key.chars().next().unwrap() == '$' {
            match key {
                "$and" => {
                    let sub_arr = crate::try_unwrap_array!("$and", value);
                    self.emit_logic_and(sub_arr.as_ref(), get_field_failed_location, not_found_branch)?;
                }

                "$or" => {
                    let sub_arr = crate::try_unwrap_array!("$and", value);
                    self.emit_logic_or(sub_arr.as_ref(), get_field_failed_location, not_found_branch)?;
                }

                "$not" => {
                    let sub_doc = crate::try_unwrap_document!("$not", value);
                    let inverse_doc = inverse_doc(sub_doc)?;
                    return self.emit_query_tuple_document(
                        key, &inverse_doc,
                        get_field_failed_location, not_found_branch
                    );
                }

                _ => {
                    return Err(DbErr::NotAValidField(key.into()));
                }
            }
        } else {
            match value {
                Value::Document(doc) => {
                    return self.emit_query_tuple_document(
                        key, doc.as_ref(),
                        get_field_failed_location, not_found_branch
                    );
                }

                Value::Array(_) => {
                    return Err(DbErr::NotAValidField(key.into()));
                }

                _ => {
                    let key_static_id = self.push_static(key.into());
                    self.emit_get_field(key_static_id, get_field_failed_location);  // push a value1

                    let value_static_id = self.push_static(value.clone());
                    self.emit_push_value(value_static_id);  // push a value2

                    self.emit(DbOp::Equal);
                    // if not equal，go to next
                    self.emit_false_jump(not_found_branch);

                    self.emit(DbOp::Pop); // pop a value2
                    self.emit(DbOp::Pop); // pop a value1
                }
            }
        }
        Ok(())
    }

    fn recursively_get_field(&mut self, key: &str, get_field_failed_location: u32) -> usize {
        let slices: Vec<&str> = key.split('.').collect();
        for slice in &slices {
            let str_ref: &str = slice;
            let current_stat_id = self.push_static(str_ref.into());
            self.emit_get_field(current_stat_id, get_field_failed_location);
        }
        slices.len()
    }

    // very complex query document
    fn emit_query_tuple_document(&mut self, key: &str, value: &Document, get_field_failed_location: u32, not_found_branch: u32) -> DbResult<()> {
        for (sub_key, sub_value) in value.iter() {
            match sub_key.as_str() {
                "$eq" => {
                    let field_size = self.recursively_get_field(key, get_field_failed_location);

                    let stat_val_id = self.push_static(sub_value.clone());
                    self.emit_push_value(stat_val_id);
                    self.emit(DbOp::Equal);

                    // if not equal，go to next
                    self.emit_false_jump(not_found_branch);

                    self.emit(DbOp::Pop2);
                    self.emit_u32((field_size + 1) as u32);
                }

                "$gt" => {
                    let field_size = self.recursively_get_field(key, get_field_failed_location);

                    let stat_val_id = self.push_static(sub_value.clone());
                    self.emit_push_value(stat_val_id);
                    self.emit(DbOp::Cmp);

                    // equal, r0 == 0
                    self.emit_false_jump(not_found_branch);
                    // greater
                    self.emit_greater_jump(not_found_branch);

                    self.emit(DbOp::Pop2);
                    self.emit_u32((field_size + 1) as u32);
                }

                "$gte" => {
                    let field_size = self.recursively_get_field(key, get_field_failed_location);

                    let stat_val_id = self.push_static(sub_value.clone());
                    self.emit_push_value(stat_val_id);
                    self.emit(DbOp::Cmp);

                    self.emit_greater_jump(not_found_branch);

                    self.emit(DbOp::Pop2);
                    self.emit_u32((field_size + 1) as u32);
                }

                // check the value is array
                "$in" => {
                    match sub_value {
                        Value::Array(_) => (),
                        _ => {
                            return Err(DbErr::NotAValidField(key.into()));
                        }
                    }

                    let field_size = self.recursively_get_field(key, get_field_failed_location);

                    let stat_val_id = self.push_static(sub_value.clone());
                    self.emit_push_value(stat_val_id);
                    self.emit(DbOp::In);

                    self.emit_false_jump(not_found_branch);

                    self.emit(DbOp::Pop2);
                    self.emit_u32((field_size + 1) as u32);
                }

                "$lt" => {
                    let field_size = self.recursively_get_field(key, get_field_failed_location);

                    let stat_val_id = self.push_static(sub_value.clone());
                    self.emit_push_value(stat_val_id);
                    self.emit(DbOp::Cmp);

                    // equal, r0 == 0
                    self.emit_false_jump(not_found_branch);
                    // less
                    self.emit_less_jump(not_found_branch);

                    self.emit(DbOp::Pop2);
                    self.emit_u32((field_size + 1) as u32);
                }

                "$lte" => {
                    let field_size = self.recursively_get_field(key, get_field_failed_location);

                    let stat_val_id = self.push_static(sub_value.clone());
                    self.emit_push_value(stat_val_id);
                    self.emit(DbOp::Cmp);

                    // less
                    self.emit_less_jump(not_found_branch);

                    self.emit(DbOp::Pop2);
                    self.emit_u32((field_size + 1) as u32);
                }

                "$ne" => {
                    let field_size = self.recursively_get_field(key, get_field_failed_location);

                    let stat_val_id = self.push_static(sub_value.clone());
                    self.emit_push_value(stat_val_id);
                    self.emit(DbOp::Equal);

                    // if equal，go to next
                    self.emit_true_jump(not_found_branch);

                    self.emit(DbOp::Pop2);
                    self.emit_u32((field_size + 1) as u32);
                }

                "$nin" => {
                    match sub_value {
                        Value::Array(_) => (),
                        _ => {
                            return Err(DbErr::NotAValidField(key.into()));
                        }
                    }

                    let field_size = self.recursively_get_field(key, get_field_failed_location);

                    let stat_val_id = self.push_static(sub_value.clone());
                    self.emit_push_value(stat_val_id);
                    self.emit(DbOp::In);

                    self.emit_true_jump(not_found_branch);

                    self.emit(DbOp::Pop2);
                    self.emit_u32((field_size + 1) as u32);
                }

                _ => {
                    return Err(DbErr::NotAValidField(sub_key.into()));
                }
            }
        }
        Ok(())
    }

    pub(super) fn emit_update_operation(&mut self, update: &Document) -> DbResult<()> {
        for (key, value) in update.iter() {
            match key.as_str() {
                "$inc" => {
                    let doc = crate::try_unwrap_document!("$inc", value);

                    self.iterate_add_op(DbOp::IncField, doc.as_ref())?;
                }

                "$set" => {
                    let doc = crate::try_unwrap_document!("$set", value);

                    self.iterate_add_op(DbOp::SetField, doc.as_ref())?;
                }

                "$max" => {
                    update_op::update_op_min_max(self, value, false)?;
                }

                "$min" => {
                    update_op::update_op_min_max(self, value, true)?;
                }

                "$mul" => {
                    let doc = crate::try_unwrap_document!("$mul", value);

                    self.iterate_add_op(DbOp::MulField, doc.as_ref())?;
                }

                "$rename" => {
                    let doc = crate::try_unwrap_document!("$set", value);

                    for (key, value) in doc.iter() {
                        let new_name = match value {
                            Value::String(new_name) => new_name,
                            t => {
                                let err = mk_field_name_type_unexpected(key, "String", t.ty_name());
                                return Err(err);
                            }
                        };

                        self.emit_rename_field(key.as_str(), new_name.as_ref());
                    }
                }

                "$unset" => {
                    let doc = crate::try_unwrap_document!("$unset", value);

                    for (key, _) in doc.iter() {
                        self.emit_unset_field(key.as_str());
                    }
                }

                _ => {
                    return Err(DbErr::UnknownUpdateOperation(key.clone()))
                }

            }
        }

        self.emit(DbOp::UpdateCurrent);

        Ok(())
    }

    fn iterate_add_op(&mut self, op: DbOp, doc: &Document) -> DbResult<()> {
        for (index, (key, value)) in doc.iter().enumerate() {
            if index == 0 && key == "_id" {
                return Err(DbErr::UnableToUpdatePrimaryKey);
            }

            let value_id = self.push_static(value.clone());
            self.emit_push_value(value_id);

            let key_id = self.push_static(Value::String(key.as_str().into()));
            self.emit(op);
            self.emit_u32(key_id);

            self.emit(DbOp::Pop);
        }
        Ok(())
    }

    #[inline]
    pub(super) fn emit_u32(&mut self, op: u32) {
        let bytes = op.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    pub(super) fn emit_open_read(&mut self, root_pid: u32) {
        self.emit(DbOp::OpenRead);
        let bytes = root_pid.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    pub(super) fn emit_open_write(&mut self, root_pid: u32) {
        self.emit(DbOp::OpenWrite);
        let bytes = root_pid.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    #[inline]
    pub(super) fn emit(&mut self, op: DbOp) {
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

    pub(super) fn emit_get_field(&mut self, static_id: u32, failed_location: u32) {
        self.emit(DbOp::GetField);
        let bytes = static_id.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
        let bytes = failed_location.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    pub(super) fn emit_push_value(&mut self, static_id: u32) {
        self.emit(DbOp::PushValue);
        let bytes = static_id.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    #[inline]
    pub(super) fn emit_false_jump(&mut self, location: u32) {
        self.emit(DbOp::IfFalse);
        let bytes = location.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    #[inline]
    pub(super) fn emit_true_jump(&mut self, location: u32) {
        self.emit(DbOp::IfTrue);
        let bytes = location.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    #[inline]
    pub(super) fn emit_less_jump(&mut self, location: u32) {
        self.emit(DbOp::IfLess);
        let bytes = location.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    #[inline]
    pub(super) fn emit_greater_jump(&mut self, location: u32) {
        self.emit(DbOp::IfGreater);
        let bytes = location.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    pub(super) fn emit_rename_field(&mut self, old_name: &str, new_name: &str) {
        let old_name_id = self.push_static(Value::String(old_name.into()));
        let new_name_id = self.push_static(Value::String(new_name.into()));
        let field_location = self.current_location();
        self.emit_get_field(old_name_id, 0);

        self.emit(DbOp::SetField);
        self.emit_u32(new_name_id);

        self.emit(DbOp::Pop);

        self.emit(DbOp::UnsetField);
        self.emit_u32(old_name_id);

        let current_loc = self.current_location();
        self.update_failed_location(field_location as usize, current_loc);
    }

    pub(super) fn emit_unset_field(&mut self, name: &str) {
        let value_id = self.push_static(Value::String(name.into()));
        self.emit(DbOp::UnsetField);
        self.emit_u32(value_id);
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

    pub(super) fn emit_goto(&mut self, location: u32) {
        self.emit(DbOp::Goto);
        let bytes = location.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    pub(super) fn emit_next(&mut self, location: u32) {
        self.emit(DbOp::Next);
        let bytes = location.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

}
