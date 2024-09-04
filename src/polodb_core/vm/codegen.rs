// Copyright 2024 Vincent Chan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


use super::label::{JumpTableRecord, Label, LabelSlot};
use crate::coll::collection_info::CollectionSpecification;
use crate::errors::{mk_invalid_query_field};
use crate::index::INDEX_PREFIX;
use crate::vm::op::DbOp;
use crate::vm::subprogram::SubProgramIndexItem;
use crate::vm::SubProgram;
use crate::{Error, Result};
use bson::spec::{BinarySubtype, ElementType};
use bson::{Array, Binary, Bson, Document};
use crate::vm::aggregation_codegen_context::{AggregationCodeGenContext, PipelineItem};
use crate::vm::global_variable::{GlobalVariable, GlobalVariableSlot};
use crate::vm::operators::OpRegistry;
use crate::vm::update_operators::{IncOperator, MaxOperator, MinOperator, MulOperator, PopOperator, PushOperator, RenameOperator, SetOperator, UnsetOperator, UpdateOperator};
use crate::vm::vm_add_fields::VmFuncAddFields;
use crate::vm::vm_count::VmFuncCount;
use crate::vm::vm_external_func::VmExternalFunc;
use crate::vm::vm_group::VmFuncGroup;
use crate::vm::vm_limit::VmFuncLimit;
use crate::vm::vm_skip::VmFuncSkip;
use crate::vm::vm_sort::VmFuncSort;
use crate::vm::vm_unset::VmFuncUnset;

const JUMP_TABLE_DEFAULT_SIZE: usize = 8;
const PATH_DEFAULT_SIZE: usize = 8;

pub(super) struct Codegen {
    program: Box<SubProgram>,
    jump_table: Vec<JumpTableRecord>,
    skip_annotation: bool,
    is_write: bool,
    paths: Vec<String>,
    op_registry: OpRegistry,
}

impl Codegen {
    pub(super) fn new(skip_annotation: bool, is_write: bool) -> Codegen {
        Codegen {
            program: Box::new(SubProgram::new()),
            jump_table: Vec::with_capacity(JUMP_TABLE_DEFAULT_SIZE),
            skip_annotation,
            is_write,
            paths: Vec::with_capacity(PATH_DEFAULT_SIZE),
            op_registry: OpRegistry,
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

    #[allow(dead_code)]
    fn emit_load_global(&mut self, global: GlobalVariable) {
        self.emit(DbOp::LoadGlobal);
        self.emit_u32(global.pos());
    }

    #[allow(dead_code)]
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
        if query_doc.is_empty() {
            self.emit(DbOp::StoreR0_2);
            self.emit_u8(1);
            return Ok(())
        }
        for (key, value) in query_doc.iter() {
            crate::path_hint!(self, key.clone(), {
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
            crate::path_hint!(self, path_msg, {
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
            crate::path_hint!(self, path_msg, {
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

                crate::path_hint!(self, "$not".to_string(), {
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
            crate::path_hint!(self, sub_key.clone(), {
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
        };

        ctx.items.push(final_pipeline_item);

        self.emit_goto(DbOp::Goto, next_label);

        for i in 0..pipeline.len() {
            self.emit_aggregation_stage(pipeline, &ctx, i)?;
        }

        self.emit_label_with_name(final_result_label, "final_result_row_fun");
        let skip_result_label = self.new_label();
        self.emit(DbOp::EqualNull);
        self.emit_goto(DbOp::IfTrue, skip_result_label);
        self.emit(DbOp::ResultRow);
        self.emit_label(skip_result_label);
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

        crate::path_hint!(self, stage_num, {
            let first_tuple = stage.iter().next().unwrap();
            let (key, value) = first_tuple;

            crate::path_hint!(self, key.to_string(), {
                match key.as_str() {
                    "$count" => {
                        let count_name = match value {
                            Bson::String(s) => s,
                            _ => {
                                return Err(Error::InvalidAggregationStage(Box::new(stage.clone())));
                            }
                        };

                        let next_fun = ctx.items[index + 1].next_label;
                        let external_func: Box<dyn VmExternalFunc> = Box::new(VmFuncCount::new(count_name.clone()));
                        self.emit_external_func(external_func, stage_ctx_item, next_fun);
                    }
                    "$group" => {
                        let next_fun = ctx.items[index + 1].next_label;
                        let external_func: Box<dyn VmExternalFunc> = VmFuncGroup::compile(
                            &mut self.paths,
                            self.op_registry.clone(),
                            value,
                        )?;
                        self.emit_external_func(external_func, stage_ctx_item, next_fun);
                    }
                    "$skip" => {
                        let next_fun = ctx.items[index + 1].next_label;
                        let external_func: Box<dyn VmExternalFunc> = VmFuncSkip::compile(&mut self.paths, value)?;
                        self.emit_external_func(external_func, stage_ctx_item, next_fun);
                    }
                    "$limit" => {
                        let next_fun = ctx.items[index + 1].next_label;
                        let external_func: Box<dyn VmExternalFunc> = VmFuncLimit::compile(&mut self.paths, value)?;
                        self.emit_external_func(external_func, stage_ctx_item, next_fun);
                    }
                    "$sort" => {
                        let next_fun = ctx.items[index + 1].next_label;
                        let external_func: Box<dyn VmExternalFunc> = VmFuncSort::compile(&mut self.paths, value)?;
                        self.emit_external_func(external_func, stage_ctx_item, next_fun);
                    }
                    "$addFields" => {
                        let next_fun = ctx.items[index + 1].next_label;
                        let external_func = VmFuncAddFields::compile(
                            &mut self.paths,
                            self.op_registry.clone(),
                            value,
                        )?;
                        self.emit_external_func(external_func, stage_ctx_item, next_fun);
                    }
                    "$unset" => {
                        let next_fun = ctx.items[index + 1].next_label;
                        let external_func: Box<dyn VmExternalFunc> = VmFuncUnset::compile(&mut self.paths, value)?;
                        self.emit_external_func(external_func, stage_ctx_item, next_fun);
                    }
                    _ => {
                        return Err(Error::UnknownAggregationOperation(key.clone()));
                    }
                };
            });
        });

        Ok(())
    }

    fn emit_external_func(&mut self, external_func: Box<dyn VmExternalFunc>, stage_ctx_item: &PipelineItem, next_fun: Label) {
        let external_func_id = self.push_external_func(external_func);
        let go_next = self.new_label();
        let loop_next = self.new_label();

        // $count_next =>
        self.emit_label(stage_ctx_item.next_label);

        self.emit(DbOp::Dup);
        self.emit_label(loop_next);
        self.emit_call_external_func_id(external_func_id, 1);
        self.emit_goto(DbOp::IfTrue, go_next);

        self.emit(DbOp::Pop);
        self.emit_ret(0);

        self.emit_label(go_next);
        self.emit_goto(DbOp::Call, next_fun);
        self.emit_u32(1);
        self.emit(DbOp::Pop);

        self.emit(DbOp::ExternalIsCompleted);
        self.emit_u32(external_func_id);
        self.emit(DbOp::PushNull);
        self.emit_goto(DbOp::IfFalse, loop_next);

        self.emit_ret(0);
    }

    fn emit_call_external_func_id(&mut self, external_func_id: u32, param_size: usize) {
        self.emit(DbOp::CallExternal);
        self.emit_u32(external_func_id);
        self.emit_u32(param_size as u32);
    }

    pub fn emit_aggregation_before_query(&mut self, ctx: &mut AggregationCodeGenContext, pipeline: &[Document]) -> Result<()> {
        for stage_doc in pipeline {
            if stage_doc.is_empty() {
                return Ok(());
            }

            let label = self.new_label();

            ctx.items.push(PipelineItem {
                next_label: label,
            });
        }
        Ok(())
    }

    pub fn emit_aggregation_before_close(&mut self, ctx: &AggregationCodeGenContext) -> Result<()> {
        for item in &ctx.items {
            let static_id = self.push_static(Bson::Null);
            self.emit_push_value(static_id);
            self.emit_goto(DbOp::Call, item.next_label);
            self.emit_u32(1);
        }

        Ok(())
    }

    pub(super) fn emit_delete_operation(&mut self) {
        self.emit(DbOp::DeleteCurrent);
    }

    pub(super) fn emit_update_operation(&mut self, update: &Document) -> Result<()> {
        self.emit(DbOp::IncR2);
        self.emit(DbOp::StoreR0_2);
        self.emit_u8(0);

        for (key, value) in update.iter() {
            crate::path_hint!(self, key.clone(), {
                self.emit_update_operation_kv(key, value)?;
            });
        }

        self.emit(DbOp::UpdateCurrent);

        Ok(())
    }

    fn push_update_operator(&mut self, operator: Box<dyn UpdateOperator>) -> usize {
        let id = self.program.update_operators.len();
        self.program.update_operators.push(operator);
        id
    }

    fn emit_update_operator(&mut self, operator: Box<dyn UpdateOperator>) {
        let id = self.push_update_operator(operator) as u32;
        self.emit(DbOp::CallUpdateOperator);
        self.emit_u32(id);
    }

    fn emit_update_operation_kv(&mut self, key: &str, value: &Bson) -> Result<()> {
        match key.as_ref() {
            "$inc" => {
                let doc = crate::try_unwrap_document!("$inc", value);

                let op = IncOperator::compile(doc.clone())?;
                self.emit_update_operator(Box::new(op));
            }

            "$set" => {
                let doc = crate::try_unwrap_document!("$set", value);

                let op = SetOperator::compile(doc.clone())?;
                self.emit_update_operator(Box::new(op));
            }

            "$max" => {
                let doc = crate::try_unwrap_document!("$max", value);

                let op = MaxOperator::compile(doc.clone())?;
                self.emit_update_operator(Box::new(op));
            }

            "$min" => {
                let doc = crate::try_unwrap_document!("$min", value);

                let op = MinOperator::compile(doc.clone())?;
                self.emit_update_operator(Box::new(op));
            }

            "$mul" => {
                let doc = crate::try_unwrap_document!("$mul", value);

                let op = MulOperator::compile(doc.clone())?;
                self.emit_update_operator(Box::new(op));
            }

            "$rename" => {
                let doc = crate::try_unwrap_document!("$set", value);

                let op = RenameOperator::compile(doc.clone())?;
                self.emit_update_operator(Box::new(op));
            }

            "$unset" => {
                let doc = crate::try_unwrap_document!("$unset", value);

                let op = UnsetOperator::compile(doc)?;
                self.emit_update_operator(Box::new(op));
            }

            "$push" => {
                let doc = crate::try_unwrap_document!("$push", value);

                let op = PushOperator::compile(doc.clone())?;
                self.emit_update_operator(Box::new(op));
            }

            "$pop" => {
                let doc = crate::try_unwrap_document!("$pop", value);

                let op = PopOperator::compile(
                    doc.clone(),
                    self.last_key().to_string(),
                    self.gen_path(),
                )?;
                self.emit_update_operator(Box::new(op));
            }

            _ => return Err(Error::UnknownUpdateOperation(key.into())),
        }

        Ok(())
    }

    #[inline]
    pub(super) fn emit_u8(&mut self, op: u8) {
        self.program.instructions.push(op);
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

    pub(super) fn push_external_func(&mut self, func: Box<dyn VmExternalFunc>) -> u32 {
        let pos = self.program.external_funcs.len() as u32;
        self.program.external_funcs.push(func);
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
