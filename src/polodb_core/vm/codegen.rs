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
            let clean_label = codegen.new_label();
            let next_element_label = codegen.new_label();
            let set_field_label = codegen.new_label();

            let rc_str: Rc<str> = key.as_str().into();
            let key_id_1 = codegen.push_static(Value::String(rc_str.clone()));
            let key_id_2 = codegen.push_static(Value::String(rc_str));
            let value_id = codegen.push_static(value.clone());

            codegen.emit_goto2(DbOp::GetField, key_id_1, &next_element_label);  // stack +1

            codegen.emit_push_value(value_id);  // stack +2

            if min {
                codegen.emit(DbOp::Less);
            } else {
                codegen.emit(DbOp::Greater);
            }
            codegen.emit_goto(DbOp::IfFalse, &set_field_label);

            codegen.emit_goto(DbOp::Goto, &clean_label);

            codegen.emit_label(&set_field_label);

            codegen.emit(DbOp::Pop);
            codegen.emit(DbOp::Pop);  // stack

            codegen.emit_push_value(value_id);

            codegen.emit(DbOp::SetField);
            codegen.emit_u32(key_id_2);

            codegen.emit(DbOp::Pop);

            codegen.emit_goto(DbOp::Goto, &next_element_label);

            codegen.emit_label(&clean_label);

            codegen.emit(DbOp::Pop);
            codegen.emit(DbOp::Pop);

            codegen.emit_label(&next_element_label);
        }

        Ok(())
    }

}

pub(super) struct Label(u32);

struct JumpToLabelRecord {
    begin_loc: u32,
    offset: u32,
    label_id: u32,
}

impl JumpToLabelRecord {

    fn new(begin_loc: u32, offset: u32, label_id: u32) -> JumpToLabelRecord {
        JumpToLabelRecord {
            begin_loc,
            offset,
            label_id,
        }
    }

}

pub(super) struct Codegen {
    program:               Box<SubProgram>,
    jump_to_label_records: Vec<JumpToLabelRecord>,
    labels:                Vec<i64>,
}

impl Codegen {

    pub(super) fn new(annotation: bool) -> Codegen {
        Codegen {
            program: Box::new(SubProgram::new(annotation)),
            jump_to_label_records: vec![],
            labels: Vec::with_capacity(16),
        }
    }

    fn unify_labels(&mut self) {
        for record in &self.jump_to_label_records {
            let pos = (record.begin_loc + record.offset) as usize;
            let target = self.labels[record.label_id as usize] as u32;
            let bytes = target.to_le_bytes();
            self.program.instructions[pos..pos + 4].copy_from_slice(&bytes);
        }
    }

    pub(super) fn take(mut self) -> SubProgram {
        self.unify_labels();
        *self.program
    }

    pub(super) fn new_label(&mut self) -> Label {
        let id = self.labels.len() as u32;
        self.labels.push(-1);
        Label(id)
    }

    pub(super) fn emit_label(&mut self, label: &Label) {
        if self.labels[label.0 as usize] >= 0 {
            unreachable!("this label has been emit");
        }
        let current_loc = self.current_location();
        self.labels[label.0 as usize] = current_loc as i64;
    }

    fn emit_query_layout_has_pkey<F>(&mut self, pkey: Value, query: &Document, result_callback: F) -> DbResult<()> where
        F: FnOnce(&mut Codegen) -> DbResult<()> {
        let close_label = self.new_label();
        let result_label = self.new_label();

        let pkey_id = self.push_static(pkey);
        self.emit_push_value(pkey_id);

        self.emit_goto(DbOp::FindByPrimaryKey, &close_label);

        self.emit_goto(DbOp::Goto, &result_label);

        self.emit_label(&close_label);
        self.emit(DbOp::Pop);
        self.emit(DbOp::Close);
        self.emit(DbOp::Halt);

        self.emit_label(&result_label);
        for (key, value) in query.iter() {
            if key == "_id" {
                continue;
            }

            let key_static_id = self.push_static(Value::String(key.as_str().into()));
            let value_static_id = self.push_static(value.clone());

            self.emit_goto2(DbOp::GetField, key_static_id, &close_label); // push a value1
            self.emit_push_value(value_static_id);  // push a value2

            self.emit(DbOp::Equal);
            // if not equal，go to next
            self.emit_goto(DbOp::IfFalse, &close_label);

            self.emit(DbOp::Pop); // pop a value2
            self.emit(DbOp::Pop); // pop a value1
        }

        result_callback(self)?;

        self.emit_goto(DbOp::Goto, &close_label);

        Ok(())
    }

    fn annotate<T: Into<String>>(&mut self, position: u32, content: T) {
        if let Some(annotation) = &mut self.program.annotation {
            annotation.annotate(position, content.into());
        }
    }

    fn annotate_here<T: Into<String>>(&mut self, content: T) {
        self.annotate(self.current_location(), content)
    }

    pub(super) fn emit_query_layout<F>(&mut self, query: &Document, result_callback: F) -> DbResult<()> where
        F: FnOnce(&mut Codegen) -> DbResult<()> {

        if let Some(id_value) = query.pkey_id() {
            if id_value.is_valid_key_type() {
                return self.emit_query_layout_has_pkey(id_value, query, result_callback);
            }
        }

        let compare_label = self.new_label();
        let next_label = self.new_label();
        let result_label = self.new_label();
        let get_field_failed_label = self.new_label();
        let not_found_label = self.new_label();
        let close_label = self.new_label();

        self.emit_goto(DbOp::Rewind, &close_label);

        self.emit_goto(DbOp::Goto, &compare_label);

        self.emit_label(&next_label);
        self.emit_goto(DbOp::Next, &compare_label);

        // <==== close cursor
        self.emit_label(&close_label);
        self.annotate_here("Close");

        self.emit(DbOp::Close);
        self.emit(DbOp::Halt);

        // <==== not this item, go to next item
        self.emit_label(&not_found_label);
        self.annotate_here("Not this item");
        self.emit(DbOp::RecoverStackPos);
        self.emit(DbOp::Pop);  // pop the current value;
        self.emit_goto(DbOp::Goto, &next_label);

        // <==== get field failed, got to next item
        self.emit_label(&get_field_failed_label);
        self.annotate_here("Get field failed");
        self.emit(DbOp::RecoverStackPos);
        self.emit(DbOp::Pop);
        self.emit_goto(DbOp::Goto, &next_label);

        // <==== result position
        // give out the result, or update the item
        self.emit_label(&result_label);
        self.annotate_here("Result");
        result_callback(self)?;
        self.emit_goto(DbOp::Goto, &next_label);

        // <==== begin to compare the top of the stack
        //
        // the top of the stack is the target document
        //
        // begin to execute compare logic
        // save the stack first
        self.emit_label(&compare_label);
        self.annotate_here("Compare");
        self.emit(DbOp::SaveStackPos);

        for (key, value) in query.iter() {
            self.emit_query_tuple(
                key, value,
                &result_label,
                &get_field_failed_label,
                &not_found_label,
            )?;
        }

        self.emit_goto(DbOp::Goto, &result_label);

        Ok(())
    }

    fn emit_logic_and(&mut self, arr: &Array, result_label: &Label, get_field_failed_label: &Label, not_found_label: &Label) -> DbResult<()> {
        for item_doc_value in arr.iter() {
            let item_doc = crate::try_unwrap_document!("$and", item_doc_value);
            for (key, value) in item_doc.iter() {
                self.emit_query_tuple(key, value, result_label, get_field_failed_label, not_found_label)?;
            }
        }

        Ok(())
    }

    fn emit_logic_or(&mut self, arr: &Array, result_label: &Label, get_field_failed_label: &Label, not_found_label: &Label) -> DbResult<()> {
        for item_doc_value in arr.iter() {
            let item_doc = crate::try_unwrap_document!("$or", item_doc_value);
            for (key, value) in item_doc.iter() {
                self.emit_query_tuple(key, value, result_label, get_field_failed_label, not_found_label)?;
            }
            self.emit_goto(DbOp::Goto, result_label);
        }

        Ok(())
    }

    // case1: "$and" | "$or" -> [ Document ]
    // case2: "$not" -> Document
    // case3: "_id" -> Document
    fn emit_query_tuple(&mut self,
                        key: &str,
                        value: &Value,
                        result_label: &Label,
                        get_field_failed_label: &Label,
                        not_found_label: &Label) -> DbResult<()> {
        if key.chars().next().unwrap() == '$' {
            match key {
                "$and" => {
                    let sub_arr = crate::try_unwrap_array!("$and", value);
                    self.emit_logic_and(
                        sub_arr.as_ref(),
                        result_label,
                        get_field_failed_label,
                        not_found_label
                    )?;
                }

                "$or" => {
                    let sub_arr = crate::try_unwrap_array!("$and", value);
                    self.emit_logic_or(
                        sub_arr.as_ref(),
                        result_label,
                        get_field_failed_label,
                        not_found_label
                    )?;
                }

                "$not" => {
                    let sub_doc = crate::try_unwrap_document!("$not", value);
                    let inverse_doc = inverse_doc(sub_doc)?;
                    return self.emit_query_tuple_document(
                        key, &inverse_doc,
                        get_field_failed_label, not_found_label
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
                        get_field_failed_label, not_found_label
                    );
                }

                Value::Array(_) => {
                    return Err(DbErr::NotAValidField(key.into()));
                }

                _ => {
                    let key_static_id = self.push_static(key.into());
                    self.emit_goto2(DbOp::GetField, key_static_id, get_field_failed_label);

                    let value_static_id = self.push_static(value.clone());
                    self.emit_push_value(value_static_id);  // push a value2

                    self.emit(DbOp::Equal);
                    // if not equal，go to next
                    self.emit_goto(DbOp::IfFalse, not_found_label);

                    self.emit(DbOp::Pop); // pop a value2
                    self.emit(DbOp::Pop); // pop a value1
                }
            }
        }
        Ok(())
    }

    fn recursively_get_field(&mut self, key: &str, get_field_failed_label: &Label) -> usize {
        let slices: Vec<&str> = key.split('.').collect();
        for slice in &slices {
            let str_ref: &str = slice;
            let current_stat_id = self.push_static(str_ref.into());
            self.emit_goto2(DbOp::GetField, current_stat_id, get_field_failed_label);
        }
        slices.len()
    }

    // very complex query document
    fn emit_query_tuple_document(&mut self, key: &str, value: &Document, get_field_failed_label: &Label, not_found_label: &Label) -> DbResult<()> {
        for (sub_key, sub_value) in value.iter() {
            match sub_key.as_str() {
                "$eq" => {
                    let field_size = self.recursively_get_field(key, get_field_failed_label);

                    let stat_val_id = self.push_static(sub_value.clone());
                    self.emit_push_value(stat_val_id);
                    self.emit(DbOp::Equal);

                    // if not equal，go to next
                    self.emit_goto(DbOp::IfFalse, not_found_label);

                    self.emit(DbOp::Pop2);
                    self.emit_u32((field_size + 1) as u32);
                }

                "$gt" => {
                    let field_size = self.recursively_get_field(key, get_field_failed_label);

                    let stat_val_id = self.push_static(sub_value.clone());
                    self.emit_push_value(stat_val_id);
                    self.emit(DbOp::Greater);

                    self.emit_goto(DbOp::IfFalse, not_found_label);

                    self.emit(DbOp::Pop2);
                    self.emit_u32((field_size + 1) as u32);
                }

                "$gte" => {
                    let field_size = self.recursively_get_field(key, get_field_failed_label);

                    let stat_val_id = self.push_static(sub_value.clone());
                    self.emit_push_value(stat_val_id);
                    self.emit(DbOp::GreaterEqual);

                    self.emit_goto(DbOp::IfFalse, not_found_label);

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

                    let field_size = self.recursively_get_field(key, get_field_failed_label);

                    let stat_val_id = self.push_static(sub_value.clone());
                    self.emit_push_value(stat_val_id);
                    self.emit(DbOp::In);

                    self.emit_goto(DbOp::IfFalse, not_found_label);

                    self.emit(DbOp::Pop2);
                    self.emit_u32((field_size + 1) as u32);
                }

                "$lt" => {
                    let field_size = self.recursively_get_field(key, get_field_failed_label);

                    let stat_val_id = self.push_static(sub_value.clone());
                    self.emit_push_value(stat_val_id);
                    self.emit(DbOp::Less);

                    self.emit_goto(DbOp::IfFalse, not_found_label);

                    self.emit(DbOp::Pop2);
                    self.emit_u32((field_size + 1) as u32);
                }

                "$lte" => {
                    let field_size = self.recursively_get_field(key, get_field_failed_label);

                    let stat_val_id = self.push_static(sub_value.clone());
                    self.emit_push_value(stat_val_id);
                    self.emit(DbOp::LessEqual);

                    // less
                    self.emit_goto(DbOp::IfFalse, not_found_label);

                    self.emit(DbOp::Pop2);
                    self.emit_u32((field_size + 1) as u32);
                }

                "$ne" => {
                    let field_size = self.recursively_get_field(key, get_field_failed_label);

                    let stat_val_id = self.push_static(sub_value.clone());
                    self.emit_push_value(stat_val_id);
                    self.emit(DbOp::Equal);

                    // if equal，go to next
                    self.emit_goto(DbOp::IfFalse, not_found_label);

                    self.emit(DbOp::Pop2);
                    self.emit_u32((field_size + 1) as u32);
                }

                "$nin" => {
                    match sub_value {
                        Value::Array(_) => (),
                        _ => return Err(DbErr::NotAValidField(key.into())),
                    }

                    let field_size = self.recursively_get_field(key, get_field_failed_label);

                    let stat_val_id = self.push_static(sub_value.clone());
                    self.emit_push_value(stat_val_id);
                    self.emit(DbOp::In);

                    self.emit_goto(DbOp::IfTrue, not_found_label);

                    self.emit(DbOp::Pop2);
                    self.emit_u32((field_size + 1) as u32);
                }

                "$size" => {
                    let expected_size = match sub_value {
                        Value::Int(i) => *i,
                        _ => return Err(DbErr::NotAValidField(key.into())),
                    };

                    let field_size = self.recursively_get_field(key, get_field_failed_label);
                    self.emit(DbOp::ArraySize);

                    let expect_size_stat_id = self.push_static(Value::from(expected_size));
                    self.emit_push_value(expect_size_stat_id);

                    self.emit(DbOp::Equal);

                    self.emit_goto(DbOp::IfFalse, not_found_label);

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

    pub(super) fn emit_push_value(&mut self, static_id: u32) {
        self.emit(DbOp::PushValue);
        let bytes = static_id.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    pub(super) fn emit_rename_field(&mut self, old_name: &str, new_name: &str) {
        let get_field_failed_label = self.new_label();
        let old_name_id = self.push_static(Value::String(old_name.into()));
        let new_name_id = self.push_static(Value::String(new_name.into()));
        self.emit_goto2(DbOp::GetField, old_name_id, &get_field_failed_label);

        self.emit(DbOp::SetField);
        self.emit_u32(new_name_id);

        self.emit(DbOp::Pop);

        self.emit(DbOp::UnsetField);
        self.emit_u32(old_name_id);

        self.emit_label(&get_field_failed_label);
    }

    pub(super) fn emit_unset_field(&mut self, name: &str) {
        let value_id = self.push_static(Value::String(name.into()));
        self.emit(DbOp::UnsetField);
        self.emit_u32(value_id);
    }

    pub(super) fn emit_goto(&mut self, op: DbOp, label: &Label) {
        let record_loc = self.current_location();
        self.emit(op);
        if self.labels[label.0 as usize] >= 0 {
            let loc = self.labels[label.0 as usize] as u32;
            let bytes = loc.to_le_bytes();
            self.program.instructions.extend_from_slice(&bytes);
            return;
        }
        let bytes = [1; 4];
        self.program.instructions.extend_from_slice(&bytes);
        self.jump_to_label_records.push(
            JumpToLabelRecord::new(record_loc, 1, label.0)
        );
    }

    pub(super) fn emit_goto2(&mut self, op: DbOp, op1: u32, label: &Label) {
        let record_loc = self.current_location();
        self.emit(op);
        let bytes: [u8; 4] = op1.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
        if self.labels[label.0 as usize] >= 0 {
            let loc = self.labels[label.0 as usize] as u32;
            let bytes: [u8; 4] = loc.to_le_bytes();
            self.program.instructions.extend_from_slice(&bytes);
            return;
        }
        let bytes2 = [1; 4];
        self.program.instructions.extend_from_slice(&bytes2);
        self.jump_to_label_records.push(
            JumpToLabelRecord::new(record_loc, 5, label.0)
        );
    }

}
