/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use super::label::{JumpTableRecord, Label, LabelSlot};
use crate::coll::collection_info::CollectionSpecification;
use crate::errors::{mk_invalid_query_field, FieldTypeUnexpectedStruct};
use crate::index::INDEX_PREFIX;
use crate::vm::op::DbOp;
use crate::vm::subprogram::SubProgramIndexItem;
use crate::vm::SubProgram;
use crate::{Error, Result};
use bson::spec::{BinarySubtype, ElementType};
use bson::{Array, Binary, Bson, Document};
use crate::vm::aggregation_codegen_context::{AggregationCodeGenContext, PipelineItem};
use crate::vm::global_variable::{GlobalVariable, GlobalVariableSlot};

const JUMP_TABLE_DEFAULT_SIZE: usize = 8;
const PATH_DEFAULT_SIZE: usize = 8;

mod update_op {
    use crate::vm::codegen::Codegen;
    use crate::vm::op::DbOp;
    use crate::Result;
    use bson::Bson;

    pub(super) fn update_op_min_max(codegen: &mut Codegen, doc: &Bson, min: bool) -> Result<()> {
        let doc = crate::try_unwrap_document!("$min", doc);

        for (key, value) in doc.iter() {
            let clean_label = codegen.new_label();
            let next_element_label = codegen.new_label();
            let set_field_label = codegen.new_label();

            let key_id_1 = codegen.push_static(Bson::from(key.clone()));
            let key_id_2 = codegen.push_static(Bson::from(key.clone()));
            let value_id = codegen.push_static(value.clone());

            codegen.emit_goto2(DbOp::GetField, key_id_1, next_element_label); // stack +1

            codegen.emit_push_value(value_id); // stack +2

            if min {
                codegen.emit(DbOp::Less);
            } else {
                codegen.emit(DbOp::Greater);
            }
            codegen.emit_goto(DbOp::IfFalse, set_field_label);

            codegen.emit_goto(DbOp::Goto, clean_label);

            codegen.emit_label(set_field_label);

            codegen.emit(DbOp::Pop);
            codegen.emit(DbOp::Pop); // stack

            codegen.emit_push_value(value_id);

            codegen.emit(DbOp::SetField);
            codegen.emit_u32(key_id_2);

            codegen.emit(DbOp::Pop);

            codegen.emit_goto(DbOp::Goto, next_element_label);

            codegen.emit_label(clean_label);

            codegen.emit(DbOp::Pop);
            codegen.emit(DbOp::Pop);

            codegen.emit_label(next_element_label);
        }

        Ok(())
    }
}

pub(super) struct Codegen {
    program: Box<SubProgram>,
    jump_table: Vec<JumpTableRecord>,
    skip_annotation: bool,
    is_write: bool,
    paths: Vec<String>,
}

macro_rules! path_hint {
    ($self:tt, $key: expr, $content:block) => {
        $self.paths.push($key);
        $content;
        $self.paths.pop();
    };
}

impl Codegen {
    pub(super) fn new(skip_annotation: bool, is_write: bool) -> Codegen {
        Codegen {
            program: Box::new(SubProgram::new()),
            jump_table: Vec::with_capacity(JUMP_TABLE_DEFAULT_SIZE),
            skip_annotation,
            is_write,
            paths: Vec::with_capacity(PATH_DEFAULT_SIZE),
        }
    }

    fn unify_labels(&mut self) {
        for record in &self.jump_table {
            let pos = (record.begin_loc + record.offset) as usize;
            let slot = &self.program.label_slots[record.label_id as usize];
            let target = slot.position();
            let bytes: [u8; 4] = target.to_le_bytes();
            self.program.instructions[pos..pos + 4].copy_from_slice(&bytes);
        }
    }

    pub(super) fn take(mut self) -> SubProgram {
        self.unify_labels();
        *self.program
    }

    #[inline]
    #[allow(dead_code)]
    pub(super) fn new_global_variable(&mut self, init_value: Bson) -> Result<GlobalVariable> {
        self.new_global_variable_impl(init_value, None)
    }

    #[inline]
    #[allow(dead_code)]
    pub(super) fn new_global_variable_with_name(&mut self, name: String, init_value: Bson) -> Result<GlobalVariable> {
        self.new_global_variable_impl(init_value, Some(name.into_boxed_str()))
    }

    fn new_global_variable_impl(&mut self, init_value: Bson, name: Option<Box<str>>) -> Result<GlobalVariable> {
        let id = self.program.global_variables.len() as u32;
        self.program.global_variables.push(GlobalVariableSlot {
            pos: 0,
            init_value,
            name,
        });
        Ok(GlobalVariable::new(id))
    }

    pub(super) fn new_label(&mut self) -> Label {
        let id = self.program.label_slots.len() as u32;
        self.program.label_slots.push(LabelSlot::Empty);
        Label::new(id)
    }

    pub(super) fn emit_label(&mut self, label: Label) {
        if !self.program.label_slots[label.u_pos()].is_empty() {
            unreachable!("this label has been emit");
        }
        let current_loc = self.current_location();
        self.emit(DbOp::Label);
        self.emit_u32(label.pos());
        self.program.label_slots[label.u_pos()] = LabelSlot::UnnamedLabel(current_loc);
    }

    fn emit_load_global(&mut self, global: GlobalVariable) {
        self.emit(DbOp::LoadGlobal);
        self.emit_u32(global.pos());
    }

    fn emit_store_global(&mut self, global: GlobalVariable) {
        self.emit(DbOp::StoreGlobal);
        self.emit_u32(global.pos());
    }

    pub(super) fn emit_label_with_name<T: Into<Box<str>>>(&mut self, label: Label, name: T) {
        if !self.program.label_slots[label.u_pos()].is_empty() {
            unreachable!("this label has been emit");
        }
        let current_loc = self.current_location();
        self.emit(DbOp::Label);
        self.emit_u32(label.pos());
        if self.skip_annotation {
            self.program.label_slots[label.u_pos()] = LabelSlot::UnnamedLabel(current_loc);
        } else {
            self.program.label_slots[label.u_pos()] =
                LabelSlot::LabelWithString(current_loc, name.into());
        }
    }

    fn emit_query_layout_has_pkey<F>(
        &mut self,
        pkey: Bson,
        query: &Document,
        result_callback: F,
    ) -> Result<()>
    where
        F: FnOnce(&mut Codegen) -> Result<()>,
    {
        let close_label = self.new_label();
        let result_label = self.new_label();

        let pkey_id = self.push_static(pkey);
        self.emit_push_value(pkey_id);

        self.emit_goto(DbOp::FindByPrimaryKey, close_label);

        self.emit_goto(DbOp::Goto, result_label);

        self.emit_label(close_label);
        self.emit(DbOp::Pop);
        self.emit(DbOp::Close);
        self.emit(DbOp::Halt);

        self.emit_label(result_label);
        for (key, value) in query.iter() {
            if key == "_id" {
                continue;
            }

            let key_static_id = self.push_static(Bson::String(key.clone()));
            let value_static_id = self.push_static(value.clone());

            self.emit_goto2(DbOp::GetField, key_static_id, close_label); // push a value1
            self.emit_push_value(value_static_id); // push a value2

            self.emit(DbOp::Equal);
            // if not equal，go to next
            self.emit_goto(DbOp::IfFalse, close_label);

            self.emit(DbOp::Pop); // pop a value2
            self.emit(DbOp::Pop); // pop a value1
        }

        result_callback(self)?;

        self.emit_goto(DbOp::Goto, close_label);

        Ok(())
    }

    pub(super) fn emit_query_layout<F>(
        &mut self,
        col_spec: &CollectionSpecification,
        query: &Document,
        result_callback: F,
        before_close: Option<Box<dyn FnOnce(&mut Codegen) -> Result<()>>>,
        is_many: bool,
    ) -> Result<()>
    where
        F: FnOnce(&mut Codegen) -> Result<()>,
    {
        let try_pkey_result = self.try_query_by_pkey(col_spec, query, result_callback)?;
        if try_pkey_result.is_none() {
            return Ok(());
        }

        let result_callback: F = try_pkey_result.unwrap();

        let try_index_result = self.try_query_by_index(col_spec, query, result_callback)?;
        if try_index_result.is_none() {
            return Ok(());
        }

        self.emit_open(col_spec._id.clone().into());

        let result_callback: F = try_index_result.unwrap();

        let compare_fun = self.new_label();
        let compare_fun_clean = self.new_label();
        let compare_label = self.new_label();
        let next_label = self.new_label();
        let result_label = self.new_label();
        let not_found_label = self.new_label();
        let close_label = self.new_label();

        self.emit_goto(DbOp::Rewind, close_label);

        self.emit_goto(DbOp::Goto, compare_label);

        self.emit_label(next_label);
        self.emit_goto(DbOp::Next, compare_label);

        // <==== close cursor
        self.emit_label_with_name(close_label, "close");

        if let Some(before_close) = before_close {
            before_close(self)?;
        }

        self.emit(DbOp::Close);
        self.emit(DbOp::Halt);

        // <==== not this item, go to next item
        self.emit_label_with_name(not_found_label, "not_this_item");
        self.emit(DbOp::Pop); // pop the current value;
        self.emit_goto(DbOp::Goto, next_label);

        // <==== result position
        // give out the result, or update the item
        self.emit_label_with_name(result_label, "result");
        result_callback(self)?;

        if is_many {
            self.emit_goto(DbOp::Goto, next_label);
        } else {
            self.emit_goto(DbOp::Goto, close_label);
        }

        // <==== begin to compare the top of the stack
        //
        // the top of the stack is the target document
        //
        // begin to execute compare logic
        // save the stack first
        self.emit_label_with_name(compare_label, "compare");
        self.emit(DbOp::Dup);
        self.emit_goto(DbOp::Call, compare_fun);
        self.emit_u32(1);
        self.emit_goto(DbOp::IfFalse, not_found_label);
        self.emit_goto(DbOp::Goto, result_label);

        self.emit_label_with_name(compare_fun, "compare_function");

        self.emit_standard_query_doc(query, result_label, compare_fun_clean)?;

        self.emit_label_with_name(compare_fun_clean, "compare_function_clean");
        self.emit_ret(0);

        Ok(())
    }

    fn try_query_by_pkey<F>(
        &mut self,
        col_spec: &CollectionSpecification,
        query: &Document,
        result_callback: F,
    ) -> Result<Option<F>>
    where
        F: FnOnce(&mut Codegen) -> Result<()>,
    {
        if let Some(id_value) = query.get("_id") {
            if id_value.element_type() != ElementType::EmbeddedDocument {
                self.emit_open(col_spec._id.clone().into());
                self.emit_query_layout_has_pkey(id_value.clone(), query, result_callback)?;
                return Ok(None);
            }
        }

        Ok(Some(result_callback))
    }

    fn try_query_by_index<F>(
        &mut self,
        col_spec: &CollectionSpecification,
        query: &Document,
        result_callback: F,
    ) -> Result<Option<F>>
    where
        F: FnOnce(&mut Codegen) -> Result<()>,
    {
        if self.is_write {
            return Ok(Some(result_callback));
        }

        let index_meta = &col_spec.indexes;
        for (index_name, index_info) in index_meta {
            let (key, _order) = index_info.keys.iter().next().unwrap();
            // the key is ellipse representation, such as "a.b.c"
            // the query is supposed to be ellipse too, such as
            // { "a.b.c": 1 }
            let test_result = query.get(key);
            if let Some(query_doc) = test_result {
                if query_doc.element_type() != ElementType::EmbeddedDocument {
                    let mut remain_query = query.clone();
                    remain_query.remove(key);

                    self.indeed_emit_query_by_index(
                        col_spec._id.as_str(),
                        index_name.as_str(),
                        query_doc,
                        &remain_query,
                        result_callback,
                    )?;
                    return Ok(None);
                }
            }
        }

        Ok(Some(result_callback))
    }

    fn indeed_emit_query_by_index<F>(
        &mut self,
        col_name: &str,
        index_name: &str,
        query_value: &Bson,
        remain_query: &Document,
        result_callback: F,
    ) -> Result<()>
    where
        F: FnOnce(&mut Codegen) -> Result<()>,
    {
        let prefix_bytes = {
            let b_prefix = Bson::String(INDEX_PREFIX.to_string());
            let b_col_name = Bson::String(col_name.to_string());
            let b_index_name = &Bson::String(index_name.to_string());

            let buf: Vec<&Bson> = vec![&b_prefix, &b_col_name, &b_index_name];
            crate::utils::bson::stacked_key(buf)?
        };

        self.emit_open(Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: prefix_bytes,
        }));

        let close_label = self.new_label();
        let result_label = self.new_label();
        let next_label = self.new_label();

        let value_id = self.push_static(query_value.clone());
        self.emit_push_value(value_id);

        let col_name_id = self.push_static(Bson::String(col_name.to_string()));
        self.emit_push_value(col_name_id);

        self.emit_goto(DbOp::FindByIndex, close_label);

        self.emit_goto(DbOp::Goto, result_label);

        self.emit_label(next_label);
        self.emit_goto(DbOp::NextIndexValue, result_label);

        self.emit_label(close_label);

        self.emit(DbOp::Pop); // pop the collection name
        self.emit(DbOp::Pop); // pop the query value

        self.emit(DbOp::Close);
        self.emit(DbOp::Halt);

        self.emit_label(result_label);
        for (key, value) in remain_query.iter() {
            let key_static_id = self.push_static(Bson::String(key.clone()));
            let value_static_id = self.push_static(value.clone());

            self.emit_goto2(DbOp::GetField, key_static_id, close_label); // push a value1
            self.emit_push_value(value_static_id); // push a value2

            self.emit(DbOp::Equal);
            // if not equal，go to next
            self.emit_goto(DbOp::IfFalse, close_label);

            self.emit(DbOp::Pop); // pop a value2
            self.emit(DbOp::Pop); // pop a value1
        }

        result_callback(self)?;

        self.emit_goto(DbOp::Goto, next_label);

        Ok(())
    }

    fn emit_standard_query_doc(
        &mut self,
        query_doc: &Document,
        result_label: Label,
        not_found_label: Label,
    ) -> Result<()> {
        for (key, value) in query_doc.iter() {
            path_hint!(self, key.clone(), {
                self.emit_query_tuple(
                    key,
                    value,
                    result_label,
                    not_found_label,
                )?;
            });
        }

        Ok(())
    }

    fn gen_path(&self) -> String {
        let mut result = String::with_capacity(32);

        for item in &self.paths {
            result.push('/');
            result.push_str(item.as_ref());
        }

        result
    }

    #[inline]
    fn last_key(&self) -> &str {
        self.paths.last().unwrap().as_str()
    }

    fn emit_logic_and(
        &mut self,
        arr: &Array,
        result_label: Label,
        not_found_label: Label,
    ) -> Result<()> {
        for (index, item_doc_value) in arr.iter().enumerate() {
            let path_msg = format!("[{}]", index);
            path_hint!(self, path_msg, {
                let item_doc = crate::try_unwrap_document!("$and", item_doc_value);
                self.emit_standard_query_doc(
                    item_doc,
                    result_label,

                    not_found_label,
                )?;
            });
        }

        Ok(())
    }

    fn emit_logic_or(
        &mut self,
        arr: &Array,
        ret_label: Label,
    ) -> Result<()> {
        let cmp_label = self.new_label();
        self.emit_goto(DbOp::Goto, cmp_label);

        let mut functions = Vec::<Label>::new();
        for (index, item_doc_value) in arr.iter().enumerate() {
            let path_msg = format!("[{}]", index);
            path_hint!(self, path_msg, {
                let item_doc = crate::try_unwrap_document!("$or", item_doc_value);

                let query_label = self.new_label();
                let ret_label = self.new_label();

                self.emit_label(query_label);
                self.emit_standard_query_doc(
                    item_doc,
                    ret_label,
                    ret_label
                )?;

                self.emit_label(ret_label);
                self.emit_ret(0);

                functions.push(query_label);
            });
        }

        self.emit_label(cmp_label);
        for fun in functions {
            self.emit_goto(DbOp::Call, fun);
            self.emit_u32(0);
            self.emit_goto(DbOp::IfTrue, ret_label);
        }

        Ok(())
    }

    // case1: "$and" | "$or" -> [ Document ]
    // case3: "_id" -> Document
    fn emit_query_tuple(
        &mut self,
        key: &str,
        value: &Bson,
        result_label: Label,
        not_found_label: Label,
    ) -> Result<()> {
        if key.chars().next().unwrap() == '$' {
            match key {
                "$and" => {
                    let sub_arr = crate::try_unwrap_array!("$and", value);
                    self.emit_logic_and(
                        sub_arr.as_ref(),
                        result_label,
                        not_found_label,
                    )?;
                }

                "$or" => {
                    let sub_arr = crate::try_unwrap_array!("$or", value);
                    self.emit_logic_or(
                        sub_arr.as_ref(),
                        not_found_label,
                    )?;
                }

                _ => {
                    return Err(Error::InvalidField(mk_invalid_query_field(
                        self.last_key().into(),
                        self.gen_path(),
                    )))
                }
            }
        } else {
            match value {
                Bson::Document(doc) => {
                    return self.emit_query_tuple_document(
                        key,
                        doc,
                        false,
                        not_found_label,
                    );
                }

                Bson::Array(_) => {
                    return Err(Error::InvalidField(mk_invalid_query_field(
                        self.last_key().into(),
                        self.gen_path(),
                    )))
                }

                _ => {
                    let key_static_id = self.push_static(key.into());
                    self.emit_goto2(DbOp::GetField, key_static_id, not_found_label);

                    let value_static_id = self.push_static(value.clone());
                    self.emit_push_value(value_static_id); // push a value2

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

    fn recursively_get_field(&mut self, key: &str, get_field_failed_label: Label) -> usize {
        let slices: Vec<&str> = key.split('.').collect();
        for slice in &slices {
            let str_ref: &str = slice;
            let current_stat_id = self.push_static(str_ref.into());
            self.emit_goto2(DbOp::GetField, current_stat_id, get_field_failed_label);
        }
        slices.len()
    }

    fn emit_logical(&mut self, op: DbOp, is_in_not: bool) {
        self.emit(op);
        if is_in_not {
            self.emit(DbOp::Not);
        }
    }

    fn emit_query_tuple_document_kv(
        &mut self,
        key: &str,
        is_in_not: bool,
        not_found_label: Label,
        sub_key: &str,
        sub_value: &Bson,
    ) -> Result<()> {
        match sub_key {
            "$eq" => {
                let field_size = self.recursively_get_field(key, not_found_label);

                let stat_val_id = self.push_static(sub_value.clone());
                self.emit_push_value(stat_val_id);
                self.emit_logical(DbOp::Equal, is_in_not);

                // if not equal，go to next
                self.emit_goto(DbOp::IfFalse, not_found_label);

                self.emit(DbOp::Pop2);
                self.emit_u32((field_size + 1) as u32);
            }

            "$gt" => {
                let field_size = self.recursively_get_field(key, not_found_label);

                let stat_val_id = self.push_static(sub_value.clone());
                self.emit_push_value(stat_val_id);
                self.emit_logical(DbOp::Greater, is_in_not);

                self.emit_goto(DbOp::IfFalse, not_found_label);

                self.emit(DbOp::Pop2);
                self.emit_u32((field_size + 1) as u32);
            }

            "$gte" => {
                let field_size = self.recursively_get_field(key, not_found_label);

                let stat_val_id = self.push_static(sub_value.clone());
                self.emit_push_value(stat_val_id);
                self.emit_logical(DbOp::GreaterEqual, is_in_not);

                self.emit_goto(DbOp::IfFalse, not_found_label);

                self.emit(DbOp::Pop2);
                self.emit_u32((field_size + 1) as u32);
            }

            // check the value is array
            "$in" => {
                match sub_value {
                    Bson::Array(_) => (),
                    _ => {
                        return Err(Error::InvalidField(mk_invalid_query_field(
                            self.last_key().into(),
                            self.gen_path(),
                        )))
                    }
                }

                let field_size = self.recursively_get_field(key, not_found_label);

                let stat_val_id = self.push_static(sub_value.clone());
                self.emit_push_value(stat_val_id);
                self.emit_logical(DbOp::In, is_in_not);

                self.emit_goto(DbOp::IfFalse, not_found_label);

                self.emit(DbOp::Pop2);
                self.emit_u32((field_size + 1) as u32);
            }

            "$lt" => {
                let field_size = self.recursively_get_field(key, not_found_label);

                let stat_val_id = self.push_static(sub_value.clone());
                self.emit_push_value(stat_val_id);
                self.emit_logical(DbOp::Less, is_in_not);

                self.emit_goto(DbOp::IfFalse, not_found_label);

                self.emit(DbOp::Pop2);
                self.emit_u32((field_size + 1) as u32);
            }

            "$lte" => {
                let field_size = self.recursively_get_field(key, not_found_label);

                let stat_val_id = self.push_static(sub_value.clone());
                self.emit_push_value(stat_val_id);
                self.emit_logical(DbOp::LessEqual, is_in_not);

                // less
                self.emit_goto(DbOp::IfFalse, not_found_label);

                self.emit(DbOp::Pop2);
                self.emit_u32((field_size + 1) as u32);
            }

            "$ne" => {
                let field_size = self.recursively_get_field(key, not_found_label);

                let stat_val_id = self.push_static(sub_value.clone());
                self.emit_push_value(stat_val_id);
                self.emit_logical(DbOp::Equal, is_in_not);

                // if equal，go to next
                self.emit_goto(DbOp::IfFalse, not_found_label);

                self.emit(DbOp::Pop2);
                self.emit_u32((field_size + 1) as u32);
            }

            "$nin" => {
                match sub_value {
                    Bson::Array(_) => (),
                    _ => {
                        return Err(Error::InvalidField(mk_invalid_query_field(
                            self.last_key().into(),
                            self.gen_path(),
                        )))
                    }
                }

                let field_size = self.recursively_get_field(key, not_found_label);

                let stat_val_id = self.push_static(sub_value.clone());
                self.emit_push_value(stat_val_id);
                self.emit_logical(DbOp::In, is_in_not);

                self.emit_goto(DbOp::IfTrue, not_found_label);

                self.emit(DbOp::Pop2);
                self.emit_u32((field_size + 1) as u32);
            }

            "$size" => {
                let expected_size = match sub_value {
                    Bson::Int64(i) => *i,
                    _ => {
                        return Err(Error::InvalidField(mk_invalid_query_field(
                            self.last_key().into(),
                            self.gen_path(),
                        )))
                    }
                };

                let field_size = self.recursively_get_field(key, not_found_label);
                self.emit(DbOp::ArraySize);

                let expect_size_stat_id = self.push_static(Bson::from(expected_size));
                self.emit_push_value(expect_size_stat_id);

                self.emit_logical(DbOp::Equal, is_in_not);

                self.emit_goto(DbOp::IfFalse, not_found_label);

                self.emit(DbOp::Pop2);
                self.emit_u32((field_size + 1) as u32);
            }

            "$regex" => {
                match sub_value {
                    Bson::RegularExpression(_) => (),
                    _ => {
                        return Err(Error::InvalidField(mk_invalid_query_field(
                            self.last_key().into(),
                            self.gen_path(),
                        )))
                    }
                }

                let field_size = self.recursively_get_field(key, not_found_label);

                let stat_val_id = self.push_static(sub_value.clone());
                self.emit_push_value(stat_val_id);

                self.emit_logical(DbOp::Regex, is_in_not);

                // if not equal，go to next
                self.emit_goto(DbOp::IfFalse, not_found_label);

                self.emit(DbOp::Pop2);
                self.emit_u32((field_size + 1) as u32);
            }

            "$not" => {
                let doc = match sub_value {
                    Bson::Document(doc) => doc,
                    _ => {
                        return Err(Error::InvalidField(mk_invalid_query_field(
                            self.last_key().into(),
                            self.gen_path(),
                        )))
                    }
                };

                path_hint!(self, "$not".to_string(), {
                    self.emit_query_tuple_document(key, doc, !is_in_not, not_found_label)?;
                });
            }

            _ => {
                return Err(Error::InvalidField(mk_invalid_query_field(
                    self.last_key().into(),
                    self.gen_path(),
                )))
            }
        }
        Ok(())
    }

    // very complex query document
    fn emit_query_tuple_document(
        &mut self,
        key: &str,
        value: &Document,
        is_in_not: bool,
        not_found_label: Label,
    ) -> Result<()> {
        for (sub_key, sub_value) in value.iter() {
            path_hint!(self, sub_key.clone(), {
                self.emit_query_tuple_document_kv(
                    key,
                    is_in_not,
                    not_found_label,
                    sub_key.as_ref(),
                    sub_value,
                )?;
            });
        }
        Ok(())
    }

    // There are two stage of compiling pipeline
    // 1. Generate the layout code of the pipeline
    // 2. Generate the implementation code of the pipeline
    pub fn emit_aggregation_pipeline(&mut self, ctx: &mut AggregationCodeGenContext, pipeline: &[Document]) -> Result<()> {
        if pipeline.is_empty() {
            self.emit(DbOp::ResultRow);
            self.emit(DbOp::Pop);
            return Ok(());
        }
        let next_label = self.new_label();

        for stage_item in &ctx.items {
            self.emit_goto(DbOp::Call, stage_item.next_label);
            self.emit_u32(1);
        }

        // the final pipeline item to emit the final result
        let final_result_label = self.new_label();
        let final_pipeline_item = PipelineItem {
            next_label: final_result_label,
            complete_label: None,
        };

        ctx.items.push(final_pipeline_item);

        self.emit_goto(DbOp::Goto, next_label);

        for i in 0..pipeline.len() {
            self.emit_aggregation_stage(pipeline, &ctx, i)?;
        }

        self.emit_label_with_name(final_result_label, "final_result_row_fun");
        self.emit(DbOp::ResultRow);
        self.emit_ret(0);

        self.emit_label_with_name(next_label, "next_item_label");
        Ok(())
    }

    // Generate the implementation code of the pipeline
    // The implementation code is a function with parameters:
    // Param 1(bool): is_the_last
    // Return value: boolean value indicating going next stage or not
    fn emit_aggregation_stage(
        &mut self,
        pipeline: &[Document],
        ctx: &AggregationCodeGenContext,
        index: usize,
    ) -> Result<()> {
        let stage = &pipeline[index];
        let stage_ctx_item = &ctx.items[index];
        let stage_num = format!("{}", index);
        if stage.is_empty() {
            return Ok(());
        }
        if stage.len() > 1 {
            return Err(Error::InvalidAggregationStage(Box::new(stage.clone())));
        }

        path_hint!(self, stage_num, {
            let first_tuple = stage.iter().next().unwrap();
            let (key, value) = first_tuple;

            match key.as_str() {
                "$count" => {
                    let count_name = match value {
                        Bson::String(s) => s,
                        _ => {
                            return Err(Error::InvalidAggregationStage(Box::new(stage.clone())));
                        }
                    };
                    let global_var = self.new_global_variable(Bson::Int64(0))?;

                    // $count_next =>
                    self.emit_label(stage_ctx_item.next_label);

                    self.emit_load_global(global_var);
                    self.emit(DbOp::Inc);
                    self.emit_store_global(global_var);
                    self.emit(DbOp::Pop);

                    self.emit_ret(0);

                    // $count_complete =>
                    self.emit_label(stage_ctx_item.complete_label.unwrap());
                    self.emit(DbOp::PushDocument);
                    self.emit_load_global(global_var);

                    let count_name_id = self.push_static(Bson::String(count_name.clone()));
                    self.emit(DbOp::SetField);
                    self.emit_u32(count_name_id);

                    self.emit(DbOp::Pop);

                    let next_fun = ctx.items[index + 1].next_label;
                    self.emit_goto(DbOp::Call, next_fun);
                    self.emit_u32(1);

                    self.emit_ret(0);
                }
                _ => {
                    return Err(Error::UnknownAggregationOperation(key.clone()));
                }
            };
        });

        Ok(())
    }

    pub fn emit_aggregation_before_query(&mut self, ctx: &mut AggregationCodeGenContext, pipeline: &[Document]) -> Result<()> {
        for stage_doc in pipeline {
            if stage_doc.is_empty() {
                return Ok(());
            }
            let first_tuple = stage_doc.iter().next().unwrap();
            let (key, _) = first_tuple;

            let label = self.new_label();
            let complete_label = match key.as_str() {
                "$count" => {
                    let complete_label = self.new_label();
                    Some(complete_label)
                }
                _ => None,
            };

            ctx.items.push(PipelineItem {
                next_label: label,
                complete_label,
            });
        }
        Ok(())
    }

    pub fn emit_aggregation_before_close(&mut self, ctx: &AggregationCodeGenContext) -> Result<()> {
        for item in &ctx.items {
            if let Some(complete_label) = item.complete_label {
                self.emit_goto(DbOp::Call, complete_label);
                self.emit_u32(0);
                return Ok(());
            }
        }

        Ok(())
    }

    pub(super) fn emit_delete_operation(&mut self) {
        self.emit(DbOp::DeleteCurrent);
    }

    pub(super) fn emit_update_operation(&mut self, update: &Document) -> Result<()> {
        for (key, value) in update.iter() {
            path_hint!(self, key.clone(), {
                self.emit_update_operation_kv(key, value)?;
            });
        }

        self.emit(DbOp::UpdateCurrent);

        Ok(())
    }

    fn emit_update_operation_kv(&mut self, key: &str, value: &Bson) -> Result<()> {
        match key.as_ref() {
            "$inc" => {
                let doc = crate::try_unwrap_document!("$inc", value);

                self.iterate_add_op(DbOp::IncField, doc)?;
            }

            "$set" => {
                let doc = crate::try_unwrap_document!("$set", value);

                self.iterate_add_op(DbOp::SetField, doc)?;
            }

            "$max" => {
                update_op::update_op_min_max(self, value, false)?;
            }

            "$min" => {
                update_op::update_op_min_max(self, value, true)?;
            }

            "$mul" => {
                let doc = crate::try_unwrap_document!("$mul", value);

                self.iterate_add_op(DbOp::MulField, doc)?;
            }

            "$rename" => {
                let doc = crate::try_unwrap_document!("$set", value);

                for (key, value) in doc.iter() {
                    let new_name = match value {
                        Bson::String(new_name) => new_name.as_str(),
                        t => {
                            let name = format!("{}", t);
                            return Err(FieldTypeUnexpectedStruct {
                                field_name: key.into(),
                                expected_ty: "String".into(),
                                actual_ty: name,
                            }
                            .into());
                        }
                    };

                    self.emit_rename_field(key.as_ref(), new_name);
                }
            }

            "$unset" => {
                let doc = crate::try_unwrap_document!("$unset", value);

                for (key, _) in doc.iter() {
                    self.emit_unset_field(key.as_ref());
                }
            }

            "$push" => {
                let doc = crate::try_unwrap_document!("$push", value);

                for (key, value) in doc.iter() {
                    self.emit_push_field(key.as_ref(), value);
                }
            }

            "$pop" => {
                let doc = crate::try_unwrap_document!("$pop", value);

                for (key, value) in doc.iter() {
                    let num = match value {
                        Bson::Int64(i) => *i,
                        _ => {
                            return Err(Error::InvalidField(mk_invalid_query_field(
                                self.last_key().into(),
                                self.gen_path(),
                            )))
                        }
                    };
                    self.emit_pop_field(
                        key.as_str(),
                        match num {
                            1 => false,
                            -1 => true,
                            _ => {
                                return Err(Error::InvalidField(mk_invalid_query_field(
                                    self.last_key().into(),
                                    self.gen_path(),
                                )))
                            }
                        },
                    );
                }
            }

            _ => return Err(Error::UnknownUpdateOperation(key.into())),
        }

        Ok(())
    }

    fn iterate_add_op(&mut self, op: DbOp, doc: &Document) -> Result<()> {
        for (index, (key, value)) in doc.iter().enumerate() {
            if index == 0 && key == "_id" {
                return Err(Error::UnableToUpdatePrimaryKey);
            }

            let value_id = self.push_static(value.clone());
            self.emit_push_value(value_id);

            let key_id = self.push_static(Bson::from(key.clone()));
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

    pub(crate) fn emit_open(&mut self, prefix: Bson) {
        self.emit(if self.is_write {
            DbOp::OpenWrite
        } else {
            DbOp::OpenRead
        });
        let id = self.push_static(prefix);
        self.emit_u32(id);
    }

    pub(crate) fn emit_ret(&mut self, return_size: u32) {
        if return_size == 0 {
            self.emit(DbOp::Ret0);
        } else {
            self.emit(DbOp::Ret);
            self.emit_u32(return_size);
        }
    }

    #[inline]
    pub(super) fn emit(&mut self, op: DbOp) {
        self.program.instructions.push(op as u8);
    }

    #[inline]
    pub(super) fn current_location(&self) -> u32 {
        self.program.instructions.len() as u32
    }

    pub(super) fn push_static(&mut self, value: Bson) -> u32 {
        let pos = self.program.static_values.len() as u32;
        self.program.static_values.push(value);
        pos
    }

    pub(super) fn push_index_info(&mut self, index_item: SubProgramIndexItem) -> u32 {
        let pos = self.program.index_infos.len() as u32;
        self.program.index_infos.push(index_item);
        pos
    }

    pub(super) fn emit_push_value(&mut self, static_id: u32) {
        self.emit(DbOp::PushValue);
        let bytes = static_id.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
    }

    pub(super) fn emit_rename_field(&mut self, old_name: &str, new_name: &str) {
        let get_field_failed_label = self.new_label();
        let old_name_id = self.push_static(Bson::String(old_name.into()));
        let new_name_id = self.push_static(Bson::String(new_name.into()));
        self.emit_goto2(DbOp::GetField, old_name_id, get_field_failed_label);

        self.emit(DbOp::SetField);
        self.emit_u32(new_name_id);

        self.emit(DbOp::Pop);

        self.emit(DbOp::UnsetField);
        self.emit_u32(old_name_id);

        self.emit_label(get_field_failed_label);
    }

    pub(super) fn emit_unset_field(&mut self, name: &str) {
        let value_id = self.push_static(Bson::String(name.into()));
        self.emit(DbOp::UnsetField);
        self.emit_u32(value_id);
    }

    pub(super) fn emit_push_field(&mut self, field_name: &str, value: &Bson) {
        let get_field_failed_label = self.new_label();
        let name_id = self.push_static(field_name.into());
        self.emit_goto2(DbOp::GetField, name_id, get_field_failed_label);

        let value_id = self.push_static(value.clone());
        self.emit(DbOp::PushValue);
        self.emit_u32(value_id);

        self.emit(DbOp::ArrayPush);

        self.emit(DbOp::Pop);

        self.emit(DbOp::SetField);
        self.emit_u32(name_id);

        self.emit(DbOp::Pop);

        self.emit_label(get_field_failed_label);
    }

    pub(super) fn emit_pop_field(&mut self, field_name: &str, is_first: bool) {
        let get_field_failed_label = self.new_label();
        let name_id = self.push_static(field_name.into());

        // <<---- push an array on stack
        self.emit_goto2(DbOp::GetField, name_id, get_field_failed_label);

        self.emit(if is_first {
            DbOp::ArrayPopFirst
        } else {
            DbOp::ArrayPopLast
        });

        self.emit(DbOp::SetField);
        self.emit_u32(name_id);

        // <<---- pop an array on stack
        self.emit(DbOp::Pop);

        self.emit_label(get_field_failed_label);
    }

    pub(super) fn emit_goto(&mut self, op: DbOp, label: Label) {
        let record_loc = self.current_location();
        self.emit(op);
        let slot = &self.program.label_slots[label.u_pos()];
        if !slot.is_empty() {
            let loc = slot.position();
            let bytes = loc.to_le_bytes();
            self.program.instructions.extend_from_slice(&bytes);
            return;
        }
        let bytes: [u8; 4] = (-1 as i32).to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
        self.jump_table
            .push(JumpTableRecord::new(record_loc, 1, label.pos()));
    }

    pub(super) fn emit_goto2(&mut self, op: DbOp, op1: u32, label: Label) {
        let record_loc = self.current_location();
        self.emit(op);
        let bytes: [u8; 4] = op1.to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes);
        let slot = &self.program.label_slots[label.u_pos()];
        if !slot.is_empty() {
            let loc = slot.position();
            let bytes: [u8; 4] = loc.to_le_bytes();
            self.program.instructions.extend_from_slice(&bytes);
            return;
        }
        let bytes2: [u8; 4] = (-1 as i32).to_le_bytes();
        self.program.instructions.extend_from_slice(&bytes2);
        self.jump_table
            .push(JumpTableRecord::new(record_loc, 5, label.pos()));
    }
}
