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

use std::cell::RefCell;
use super::label::LabelSlot;
use super::op::DbOp;
use crate::coll::collection_info::{CollectionSpecification, IndexInfo};
use crate::utils::str::escape_binary_to_string;
use crate::vm::codegen::Codegen;
use crate::{Result};
use bson::{Bson, Document};
use indexmap::IndexMap;
use std::fmt;
use std::rc::Rc;
use crate::errors::FieldTypeUnexpectedStruct;
use crate::vm::aggregation_codegen_context::AggregationCodeGenContext;
use crate::vm::global_variable::GlobalVariableSlot;
use crate::vm::vm_external_func::VmExternalFunc;

pub(crate) struct SubProgramIndexItem {
    pub col_name: String,
    pub indexes: IndexMap<String, IndexInfo>,
}

pub(crate) struct SubProgram {
    pub(super) static_values: Vec<Bson>,
    pub(super) instructions: Vec<u8>,
    pub(super) global_variables: Vec<GlobalVariableSlot>,
    pub(super) label_slots: Vec<LabelSlot>,
    pub(super) index_infos: Vec<SubProgramIndexItem>,
    pub(crate) external_funcs: Vec<Box<dyn VmExternalFunc>>,
}

impl SubProgram {
    pub(super) fn new() -> SubProgram {
        SubProgram {
            static_values: Vec::with_capacity(32),
            instructions: Vec::with_capacity(256),
            global_variables: Vec::with_capacity(16),
            label_slots: Vec::with_capacity(32),
            index_infos: Vec::new(),
            external_funcs: Vec::new(),
        }
    }

    pub(crate) fn compile_empty_query() -> SubProgram {
        let mut codegen = Codegen::new(true, false);

        codegen.emit(DbOp::Halt);

        codegen.take()
    }

    pub(crate) fn compile_query(
        col_spec: &CollectionSpecification,
        query: &Document,
        skip_annotation: bool,
    ) -> Result<SubProgram> {
        if query.is_empty() {
            return SubProgram::compile_query_all(col_spec, skip_annotation);
        }

        let mut codegen = Codegen::new(skip_annotation, false);

        codegen.emit_query_layout(
            col_spec,
            query,
            |codegen| -> Result<()> {
                codegen.emit(DbOp::ResultRow);
                codegen.emit(DbOp::Pop);
                Ok(())
            },
            None,
            true,
        )?;

        Ok(codegen.take())
    }

    pub(crate) fn compile_update(
        col_spec: &CollectionSpecification,
        query: &Document,
        update: &Document,
        skip_annotation: bool,
        is_many: bool,
    ) -> Result<SubProgram> {
        let mut codegen = Codegen::new(skip_annotation, true);

        let has_indexes = !col_spec.indexes.is_empty();
        let index_item_id: u32 = if has_indexes {
            codegen.push_index_info(SubProgramIndexItem {
                col_name: col_spec._id.to_string(),
                indexes: col_spec.indexes.clone(),
            })
        } else {
            u32::MAX
        };

        codegen.emit_query_layout(
            col_spec,
            query,
            |codegen| -> Result<()> {
                if has_indexes {
                    codegen.emit(DbOp::DeleteIndex);
                    codegen.emit_u32(index_item_id);
                }

                codegen.emit_update_operation(update)?;

                if has_indexes {
                    codegen.emit(DbOp::InsertIndex);
                    codegen.emit_u32(index_item_id);
                }

                codegen.emit(DbOp::Pop);
                Ok(())
            },
            None,
            is_many,
        )?;

        Ok(codegen.take())
    }

    pub(crate) fn compile_delete(
        col_spec: &CollectionSpecification,
        col_name: &str,
        query: Option<&Document>,
        skip_annotation: bool,
        is_many: bool,
    ) -> Result<SubProgram> {
        let mut codegen = Codegen::new(skip_annotation, true);

        let has_indexes = !col_spec.indexes.is_empty();
        let index_item_id: u32 = if has_indexes {
            codegen.push_index_info(SubProgramIndexItem {
                col_name: col_spec._id.to_string(),
                indexes: col_spec.indexes.clone(),
            })
        } else {
            u32::MAX
        };

        codegen.emit_open(col_name.into());

        codegen.emit_query_layout(
            col_spec,
            query.unwrap(),
            |codegen| -> Result<()> {
                if has_indexes {
                    codegen.emit(DbOp::DeleteIndex);
                    codegen.emit_u32(index_item_id);
                }

                codegen.emit_delete_operation();
                codegen.emit(DbOp::Pop);
                Ok(())
            },
            None,
            is_many,
        )?;

        Ok(codegen.take())
    }

    // TODO: need test
    pub(crate) fn compile_delete_all(
        col_spec: &CollectionSpecification,
        col_name: &str,
        skip_annotation: bool,
    ) -> Result<SubProgram> {
        let mut codegen = Codegen::new(skip_annotation, true);

        let has_indexes = !col_spec.indexes.is_empty();
        let index_item_id: u32 = if has_indexes {
            codegen.push_index_info(SubProgramIndexItem {
                col_name: col_spec._id.to_string(),
                indexes: col_spec.indexes.clone(),
            })
        } else {
            u32::MAX
        };

        let result_label = codegen.new_label();
        let next_label = codegen.new_label();
        let close_label = codegen.new_label();

        codegen.emit_open(col_name.into());

        codegen.emit_goto(DbOp::Rewind, close_label);

        codegen.emit_goto(DbOp::Goto, result_label);

        codegen.emit_label(next_label);
        codegen.emit_goto(DbOp::Next, result_label);

        codegen.emit_label(close_label);
        codegen.emit(DbOp::Close);
        codegen.emit(DbOp::Halt);

        codegen.emit_label(result_label);
        if has_indexes {
            codegen.emit(DbOp::DeleteIndex);
            codegen.emit_u32(index_item_id);
        }
        codegen.emit_delete_operation();
        codegen.emit(DbOp::Pop);

        codegen.emit_goto(DbOp::Goto, next_label);

        Ok(codegen.take())
    }

    pub(crate) fn compile_query_all(
        col_spec: &CollectionSpecification,
        skip_annotation: bool,
    ) -> Result<SubProgram> {
        SubProgram::compile_query_all_by_name(col_spec.name(), skip_annotation)
    }

    pub(crate) fn compile_query_all_by_name(
        col_name: &str,
        skip_annotation: bool,
    ) -> Result<SubProgram> {
        let mut codegen = Codegen::new(skip_annotation, false);
        let result_label = codegen.new_label();
        let next_label = codegen.new_label();
        let close_label = codegen.new_label();

        codegen.emit_open(col_name.into());

        codegen.emit_goto(DbOp::Rewind, close_label);

        codegen.emit_goto(DbOp::Goto, result_label);

        codegen.emit_label(next_label);
        codegen.emit_goto(DbOp::Next, result_label);

        codegen.emit_label(close_label);
        codegen.emit(DbOp::Close);
        codegen.emit(DbOp::Halt);

        codegen.emit_label(result_label);
        codegen.emit(DbOp::ResultRow);
        codegen.emit(DbOp::Pop);

        codegen.emit_goto(DbOp::Goto, next_label);

        Ok(codegen.take())
    }

    // If the first pipeline is $match, the process can be optimized.
    pub(crate) fn compile_aggregate(
        col_spec: &CollectionSpecification,
        pipeline: impl IntoIterator<Item = Document>,
        skip_annotation: bool,
    ) -> Result<SubProgram> {
        let pipeline_vec: Vec<Document> = pipeline.into_iter().collect();
        if pipeline_vec.is_empty() {
            return SubProgram::compile_query_all(col_spec, skip_annotation);
        }

        let first = pipeline_vec.first().unwrap();
        if first.len() == 1 && first.contains_key("$match") {
            return SubProgram::compile_aggregate_with_match(col_spec, pipeline_vec, skip_annotation);
        }

        let mut codegen = Codegen::new(skip_annotation, false);
        let result_label = codegen.new_label();
        let next_label = codegen.new_label();
        let close_label = codegen.new_label();

        let mut ctx = AggregationCodeGenContext::default();

        // set up the slots for the aggregation pipeline
        codegen.emit_aggregation_before_query(&mut ctx, &pipeline_vec)?;

        codegen.emit_open(col_spec.name().into());

        codegen.emit_goto(DbOp::Rewind, close_label);

        codegen.emit_goto(DbOp::Goto, result_label);

        codegen.emit_label(next_label);
        codegen.emit_goto(DbOp::Next, result_label);

        // emit the result row
        codegen.emit_label(close_label);
        codegen.emit_aggregation_before_close(&ctx)?;
        codegen.emit(DbOp::Close);
        codegen.emit(DbOp::Halt);

        codegen.emit_label(result_label);
        codegen.emit_aggregation_pipeline(&mut ctx, &pipeline_vec)?;

        codegen.emit_goto(DbOp::Goto, next_label);

        Ok(codegen.take())
    }

    // If the first pipeline is $match, the process will leverage the index.
    pub(crate) fn compile_aggregate_with_match(
        col_spec: &CollectionSpecification,
        pipeline_vec: Vec<Document>,
        skip_annotation: bool,
    ) -> Result<SubProgram> {
        let mut codegen = Codegen::new(skip_annotation, false);
        let first_doc = pipeline_vec.first().unwrap();
        let query_doc_value = first_doc.get("$match").unwrap();
        let query_doc = match query_doc_value {
            Bson::Document(doc) => doc,
            t => {
                let name = format!("{}", t);
                return Err(FieldTypeUnexpectedStruct {
                    field_name: "$match".to_string(),
                    expected_ty: "Document".to_string(),
                    actual_ty: name,
                }.into());
            },
        };

        let ctx_ref = Rc::new(RefCell::new(AggregationCodeGenContext::default()));
        let ctx_ref2 = ctx_ref.clone();
        {
            let mut ctx = ctx_ref.borrow_mut();
            codegen.emit_aggregation_before_query(&mut ctx, &pipeline_vec[1..])?;
        }
        codegen.emit_query_layout(
            col_spec,
            query_doc,
            |codegen: &mut Codegen| -> Result<()> {
                let ctx_ref = ctx_ref.clone();
                let mut ctx = ctx_ref.borrow_mut();
                codegen.emit_aggregation_pipeline(&mut ctx, &pipeline_vec[1..])?;
                Ok(())
            },
            Some(Box::new(move |codegen: &mut Codegen| -> Result<()> {
                let ctx = ctx_ref2.borrow_mut();
                codegen.emit_aggregation_before_close(&ctx)?;
                Ok(())
            })),
            true,
        )?;

        Ok(codegen.take())
    }

}

fn open_bson_to_str(val: &Bson) -> Result<String> {
    let (str, is_bin) = match val {
        Bson::String(s) => (s.clone(), false),
        Bson::Binary(bin) => (escape_binary_to_string(bin.bytes.as_slice())?, true),
        _ => panic!("unexpected bson value: {:?}", val),
    };

    let mut result = if is_bin {
        "b\"".to_string()
    } else {
        "\"".to_string()
    };
    result.extend(str.chars());
    result.extend("\"".chars());

    Ok(result)
}

impl fmt::Display for SubProgram {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {

        for (index, global_var) in self.global_variables.iter().enumerate() {
            writeln!(f, "${} = {:?}", index, global_var.init_value)?;
        }

        if !self.global_variables.is_empty() {
            writeln!(f)?;
        }

        unsafe {
            let begin = self.instructions.as_ptr();
            let mut pc: usize = 0;
            while pc < self.instructions.len() {
                let op = begin.add(pc).cast::<DbOp>().read();
                match op {
                    DbOp::Goto => {
                        let location = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: Goto({})", pc, location)?;
                        pc += 5;
                    }

                    DbOp::Label => {
                        writeln!(f)?;
                        let label_id = begin.add(pc + 1).cast::<u32>().read();
                        match &self.label_slots[label_id as usize] {
                            LabelSlot::Empty => unreachable!(),
                            LabelSlot::UnnamedLabel(_) => {
                                writeln!(f, "{}: Label({})", pc, label_id)?
                            }
                            LabelSlot::LabelWithString(_, name) => {
                                writeln!(f, "{}: Label({}, \"{}\")", pc, label_id, name)?
                            }
                        }
                        pc += 5;
                    }

                    DbOp::Inc => {
                        writeln!(f, "{}: Inc", pc)?;
                        pc += 1;
                    }

                    DbOp::IncR2 => {
                        writeln!(f, "{}: IncR2", pc)?;
                        pc += 1;
                    }

                    DbOp::IfTrue => {
                        let location = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: TrueJump({})", pc, location)?;
                        pc += 5;
                    }

                    DbOp::IfFalse => {
                        let location = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: FalseJump({})", pc, location)?;
                        pc += 5;
                    }

                    DbOp::Rewind => {
                        let location = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: Rewind({})", pc, location)?;
                        pc += 5;
                    }

                    DbOp::FindByPrimaryKey => {
                        let location = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: FindByPrimaryKey({})", pc, location)?;
                        pc += 5;
                    }

                    DbOp::FindByIndex => {
                        let location = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: FindByIndex({})", pc, location)?;
                        pc += 5;
                    }

                    DbOp::Next => {
                        let location = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: Next({})", pc, location)?;
                        pc += 5;
                    }

                    DbOp::NextIndexValue => {
                        let location = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: NextIndexValue({})", pc, location)?;
                        pc += 5;
                    }

                    DbOp::PushValue => {
                        let index = begin.add(pc + 1).cast::<u32>().read();
                        let val = &self.static_values[index as usize];
                        writeln!(f, "{}: PushValue({})", pc, val)?;
                        pc += 5;
                    }

                    DbOp::PushTrue => {
                        writeln!(f, "{}: PushTrue", pc)?;
                        pc += 1;
                    }

                    DbOp::PushFalse => {
                        writeln!(f, "{}: PushFalse", pc)?;
                        pc += 1;
                    }

                    DbOp::PushDocument => {
                        writeln!(f, "{}: PushDocument", pc)?;
                        pc += 1;
                    }

                    DbOp::PushNull => {
                        writeln!(f, "{}: PushNull", pc)?;
                        pc += 1;
                    }

                    DbOp::PushR0 => {
                        writeln!(f, "{}: PushR0", pc)?;
                        pc += 1;
                    }

                    DbOp::StoreR0 => {
                        writeln!(f, "{}: StoreR0", pc)?;
                        pc += 1;
                    }

                    DbOp::StoreR0_2 => {
                        let value = begin.add(pc + 1).read();
                        writeln!(f, "{}: StoreR0_2({})", pc, value)?;
                        pc += 2;
                    }

                    DbOp::UpdateCurrent => {
                        writeln!(f, "{}: UpdateCurrent", pc)?;
                        pc += 1;
                    }

                    DbOp::DeleteCurrent => {
                        writeln!(f, "{}: DeleteCurrent", pc)?;
                        pc += 1;
                    }

                    DbOp::InsertIndex => {
                        let index = begin.add(pc + 1).cast::<u32>().read();
                        let index_info = &self.index_infos[index as usize];
                        writeln!(f, "{}: InsertIndex(\"{}\")", pc, index_info.col_name)?;
                        pc += 5;
                    }

                    DbOp::DeleteIndex => {
                        let index = begin.add(pc + 1).cast::<u32>().read();
                        let index_info = &self.index_infos[index as usize];
                        writeln!(f, "{}: DeleteIndex(\"{}\")", pc, index_info.col_name)?;
                        pc += 5;
                    }

                    DbOp::Dup => {
                        writeln!(f, "{}: Dup", pc)?;
                        pc += 1;
                    }

                    DbOp::Pop => {
                        writeln!(f, "{}: Pop", pc)?;
                        pc += 1;
                    }

                    DbOp::Pop2 => {
                        let index = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: Pop2({})", pc, index)?;
                        pc += 5;
                    }

                    DbOp::Equal => {
                        writeln!(f, "{}: Equal", pc)?;
                        pc += 1;
                    }

                    DbOp::Greater => {
                        writeln!(f, "{}: Greater", pc)?;
                        pc += 1;
                    }

                    DbOp::GreaterEqual => {
                        writeln!(f, "{}: GreaterEqual", pc)?;
                        pc += 1;
                    }

                    DbOp::Less => {
                        writeln!(f, "{}: Less", pc)?;
                        pc += 1;
                    }

                    DbOp::LessEqual => {
                        writeln!(f, "{}: LessEqual", pc)?;
                        pc += 1;
                    }

                    DbOp::Regex => {
                        writeln!(f, "{}: Regex", pc)?;
                        pc += 1;
                    }

                    DbOp::Not => {
                        writeln!(f, "{}: Not", pc)?;
                        pc += 1;
                    }

                    DbOp::In => {
                        writeln!(f, "{}: In", pc)?;
                        pc += 1;
                    }

                    DbOp::EqualNull => {
                        writeln!(f, "{}: EqualNull", pc)?;
                        pc += 1;
                    }

                    DbOp::OpenRead => {
                        let idx = begin.add(pc + 1).cast::<u32>().read();
                        let value = &self.static_values[idx as usize];
                        let value_str = open_bson_to_str(value).unwrap();
                        writeln!(f, "{}: OpenRead({})", pc, value_str)?;
                        pc += 5;
                    }

                    DbOp::OpenWrite => {
                        let idx = begin.add(pc + 1).cast::<u32>().read();
                        let value = &self.static_values[idx as usize];
                        let value_str = open_bson_to_str(value).unwrap();
                        writeln!(f, "{}: OpenWrite({})", pc, value_str)?;
                        pc += 5;
                    }

                    DbOp::ResultRow => {
                        writeln!(f, "{}: ResultRow", pc)?;
                        pc += 1;
                    }

                    DbOp::Close => {
                        writeln!(f, "{}: Close", pc)?;
                        pc += 1;
                    }

                    DbOp::Halt => {
                        writeln!(f, "{}: Halt", pc)?;
                        pc += 1;
                    }

                    DbOp::GetField => {
                        let static_id = begin.add(pc + 1).cast::<u32>().read();
                        let val = &self.static_values[static_id as usize];
                        let location = begin.add(pc + 5).cast::<u32>().read();
                        writeln!(f, "{}: GetField({}, {})", pc, val, location)?;
                        pc += 9;
                    }

                    DbOp::IncField => {
                        let static_id = begin.add(pc + 1).cast::<u32>().read();
                        let val = &self.static_values[static_id as usize];
                        writeln!(f, "{}: IncField({})", pc, val)?;
                        pc += 5;
                    }

                    DbOp::MulField => {
                        let static_id = begin.add(pc + 1).cast::<u32>().read();
                        let val = &self.static_values[static_id as usize];
                        writeln!(f, "{}: MulField({})", pc, val)?;
                        pc += 5;
                    }

                    DbOp::SetField => {
                        let static_id = begin.add(pc + 1).cast::<u32>().read();
                        let val = &self.static_values[static_id as usize];
                        writeln!(f, "{}: SetField({})", pc, val)?;
                        pc += 5;
                    }

                    DbOp::ArraySize => {
                        writeln!(f, "{}: ArraySize", pc)?;
                        pc += 1;
                    }

                    DbOp::ArrayPush => {
                        writeln!(f, "{}: ArrayPush", pc)?;
                        pc += 1;
                    }

                    DbOp::UnsetField => {
                        let static_id = begin.add(pc + 1).cast::<u32>().read();
                        let val = &self.static_values[static_id as usize];
                        writeln!(f, "{}: UnsetField({})", pc, val)?;
                        pc += 5;
                    }

                    DbOp::Call => {
                        let label_id = begin.add(pc + 1).cast::<u32>().read();
                        let param_size = begin.add(pc + 5).cast::<u32>().read();
                        writeln!(f, "{}: Call({}, {})", pc, label_id, param_size)?;
                        pc += 9;
                    }

                    DbOp::CallExternal => {
                        let func_id = begin.add(pc + 1).cast::<u32>().read();
                        let external_func = &self.external_funcs[func_id as usize];
                        let param_size = begin.add(pc + 5).cast::<u32>().read();
                        writeln!(f, "{}: CallExternal(${}, {})", pc, external_func.name(), param_size)?;
                        pc += 9;
                    }

                    DbOp::ExternalIsCompleted => {
                        let func_id = begin.add(pc + 1).cast::<u32>().read();
                        let external_func = &self.external_funcs[func_id as usize];
                        writeln!(f, "{}: ExternalIsCompleted(${})", pc, external_func.name())?;
                        pc += 5;
                    }

                    DbOp::Ret0 => {
                        writeln!(f, "{}: Ret0", pc)?;
                        pc += 1;
                    }

                    DbOp::Ret => {
                        let return_size = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: Ret({})", pc, return_size)?;
                        pc += 5;
                    }

                    DbOp::IfFalseRet => {
                        let return_size = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: FalseRet({})", pc, return_size)?;
                        pc += 5;
                    }

                    DbOp::SaveStackPos => {
                        writeln!(f, "{}: SaveStackPos", pc)?;
                        pc += 1;
                    }

                    DbOp::RecoverStackPos => {
                        writeln!(f, "{}: RecoverStackPos", pc)?;
                        pc += 1;
                    }

                    DbOp::LoadGlobal => {
                        let global_id = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: LoadGlobal(${})", pc, global_id)?;
                        pc += 5;
                    }

                    DbOp::StoreGlobal => {
                        let global_id = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: StoreGlobal(${})", pc, global_id)?;
                        pc += 5;
                    }

                    _ => {
                        writeln!(f, "{}: Unknown", pc)?;
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::coll::collection_info::{CollectionSpecification, IndexInfo};
    use crate::vm::SubProgram;
    use bson::{doc, Regex};
    use indexmap::indexmap;
    use polodb_line_diff::assert_eq;
    use crate::Error;

    #[inline]
    fn new_spec<T: Into<String>>(name: T) -> CollectionSpecification {
        CollectionSpecification::new(name.into(), uuid::Uuid::new_v4())
    }

    #[test]
    fn print_program() {
        // let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let col_spec = new_spec("test");
        let program = SubProgram::compile_query_all(&col_spec, false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = "Program:

0: OpenRead(\"test\")
5: Rewind(25)
10: Goto(32)

15: Label(1)
20: Next(32)

25: Label(2)
30: Close
31: Halt

32: Label(0)
37: ResultRow
38: Pop
39: Goto(15)
";

        assert_eq!(expect, actual);
    }

    #[test]
    fn print_query() {
        let test_doc = doc! {
            "name": "Vincent Chan",
            "age": 32,
        };
        let col_spec = new_spec("test");
        let program = SubProgram::compile_query(&col_spec, &test_doc, false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead("test")
5: Rewind(25)
10: Goto(55)

15: Label(3)
20: Next(55)

25: Label(6, "close")
30: Close
31: Halt

32: Label(5, "not_this_item")
37: Pop
38: Goto(15)

43: Label(4, "result")
48: ResultRow
49: Pop
50: Goto(15)

55: Label(2, "compare")
60: Dup
61: Call(80, 1)
70: FalseJump(32)
75: Goto(43)

80: Label(0, "compare_function")
85: GetField("name", 129)
94: PushValue("Vincent Chan")
99: Equal
100: FalseJump(129)
105: Pop
106: Pop
107: GetField("age", 129)
116: PushValue(32)
121: Equal
122: FalseJump(129)
127: Pop
128: Pop

129: Label(1, "compare_function_clean")
134: Ret0
"#;
        assert_eq!(expect, actual)
    }

    #[test]
    fn print_query_embedded_document() {
        let query_doc = doc! {
            "info.color": "yellow",
        };
        let col_spec = new_spec("test");
        let program = SubProgram::compile_query(&col_spec, &query_doc, false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead("test")
5: Rewind(25)
10: Goto(55)

15: Label(3)
20: Next(55)

25: Label(6, "close")
30: Close
31: Halt

32: Label(5, "not_this_item")
37: Pop
38: Goto(15)

43: Label(4, "result")
48: ResultRow
49: Pop
50: Goto(15)

55: Label(2, "compare")
60: Dup
61: Call(80, 1)
70: FalseJump(32)
75: Goto(43)

80: Label(0, "compare_function")
85: GetField("info.color", 107)
94: PushValue("yellow")
99: Equal
100: FalseJump(107)
105: Pop
106: Pop

107: Label(1, "compare_function_clean")
112: Ret0
"#;
        assert_eq!(expect, actual)
    }

    #[test]
    fn print_query_by_primary_key() {
        let col_spec = new_spec("test");
        let test_doc = doc! {
            "_id": 6,
            "age": 32,
        };
        let program = SubProgram::compile_query(&col_spec, &test_doc, false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead("test")
5: PushValue(6)
10: FindByPrimaryKey(20)
15: Goto(28)

20: Label(0)
25: Pop
26: Close
27: Halt

28: Label(1)
33: GetField("age", 20)
42: PushValue(32)
47: Equal
48: FalseJump(20)
53: Pop
54: Pop
55: ResultRow
56: Pop
57: Goto(20)
"#;
        assert_eq!(expect, actual)
    }

    #[test]
    fn print_query_by_index() {
        let mut col_spec = new_spec("test");

        col_spec.indexes.insert(
            "age_1".into(),
            IndexInfo {
                keys: indexmap! {
                    "age".into() => 1,
                },
                options: None,
            },
        );

        let test_doc = doc! {
            "age": 32,
            "name": "Vincent Chan",
        };

        let program = SubProgram::compile_query(&col_spec, &test_doc, false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead(b"\x02$I\x00\x02test\x00\x02age_1\x00")
5: PushValue(32)
10: PushValue("test")
15: FindByIndex(35)
20: Goto(44)

25: Label(2)
30: NextIndexValue(44)

35: Label(0)
40: Pop
41: Pop
42: Close
43: Halt

44: Label(1)
49: GetField("name", 35)
58: PushValue("Vincent Chan")
63: Equal
64: FalseJump(35)
69: Pop
70: Pop
71: ResultRow
72: Pop
73: Goto(25)
"#;
        assert_eq!(expect, actual);
    }

    #[test]
    fn query_by_logic_and() {
        let col_spec = new_spec("test");
        let test_doc = doc! {
            "$and": [
                doc! {
                    "_id": 6,
                },
                doc! {
                    "age": 32,
                },
            ],
        };
        let program = SubProgram::compile_query(&col_spec, &test_doc, false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead("test")
5: Rewind(25)
10: Goto(55)

15: Label(3)
20: Next(55)

25: Label(6, "close")
30: Close
31: Halt

32: Label(5, "not_this_item")
37: Pop
38: Goto(15)

43: Label(4, "result")
48: ResultRow
49: Pop
50: Goto(15)

55: Label(2, "compare")
60: Dup
61: Call(80, 1)
70: FalseJump(32)
75: Goto(43)

80: Label(0, "compare_function")
85: GetField("_id", 129)
94: PushValue(6)
99: Equal
100: FalseJump(129)
105: Pop
106: Pop
107: GetField("age", 129)
116: PushValue(32)
121: Equal
122: FalseJump(129)
127: Pop
128: Pop

129: Label(1, "compare_function_clean")
134: Ret0
"#;
        assert_eq!(expect, actual)
    }

    #[test]
    fn print_logic_or() {
        let col_spec = new_spec("test");
        let test_doc = doc! {
            "$or": [
                doc! {
                    "age": 11,
                },
                doc! {
                    "age": 12,
                },
            ],
        };
        let program = SubProgram::compile_query(&col_spec, &test_doc, false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead("test")
5: Rewind(25)
10: Goto(55)

15: Label(3)
20: Next(55)

25: Label(6, "close")
30: Close
31: Halt

32: Label(5, "not_this_item")
37: Pop
38: Goto(15)

43: Label(4, "result")
48: ResultRow
49: Pop
50: Goto(15)

55: Label(2, "compare")
60: Dup
61: Call(80, 1)
70: FalseJump(32)
75: Goto(43)

80: Label(0, "compare_function")
85: Goto(156)

90: Label(8)
95: GetField("age", 117)
104: PushValue(11)
109: Equal
110: FalseJump(117)
115: Pop
116: Pop

117: Label(9)
122: Ret0

123: Label(10)
128: GetField("age", 150)
137: PushValue(12)
142: Equal
143: FalseJump(150)
148: Pop
149: Pop

150: Label(11)
155: Ret0

156: Label(7)
161: Call(90, 0)
170: TrueJump(189)
175: Call(123, 0)
184: TrueJump(189)

189: Label(1, "compare_function_clean")
194: Ret0
"#;
        assert_eq!(expect, actual);
    }

    #[test]
    fn print_not_expression() {
        let col_spec = new_spec("test");
        let test_doc = doc! {
            "price": {
                "$not": {
                    "$gt": 100,
                },
            }
        };
        let program = SubProgram::compile_query(&col_spec, &test_doc, false).unwrap();
        let actual = format!("Program:\n\n{}", program);
        let expect = r#"Program:

0: OpenRead("test")
5: Rewind(25)
10: Goto(55)

15: Label(3)
20: Next(55)

25: Label(6, "close")
30: Close
31: Halt

32: Label(5, "not_this_item")
37: Pop
38: Goto(15)

43: Label(4, "result")
48: ResultRow
49: Pop
50: Goto(15)

55: Label(2, "compare")
60: Dup
61: Call(80, 1)
70: FalseJump(32)
75: Goto(43)

80: Label(0, "compare_function")
85: GetField("price", 111)
94: PushValue(100)
99: Greater
100: Not
101: FalseJump(111)
106: Pop2(2)

111: Label(1, "compare_function_clean")
116: Ret0
"#;
        assert_eq!(expect, actual);
    }

    #[test]
    fn print_complex_print() {
        let col_spec = new_spec("test");
        let test_doc = doc! {
            "age": doc! {
                "$gt": 3,
            },
            "child.age": doc! {
                "$in": [ 1, 2 ],
            },
        };
        let program = SubProgram::compile_query(&col_spec, &test_doc, false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead("test")
5: Rewind(25)
10: Goto(55)

15: Label(3)
20: Next(55)

25: Label(6, "close")
30: Close
31: Halt

32: Label(5, "not_this_item")
37: Pop
38: Goto(15)

43: Label(4, "result")
48: ResultRow
49: Pop
50: Goto(15)

55: Label(2, "compare")
60: Dup
61: Call(80, 1)
70: FalseJump(32)
75: Goto(43)

80: Label(0, "compare_function")
85: GetField("age", 144)
94: PushValue(3)
99: Greater
100: FalseJump(144)
105: Pop2(2)
110: GetField("child", 144)
119: GetField("age", 144)
128: PushValue([1, 2])
133: In
134: FalseJump(144)
139: Pop2(3)

144: Label(1, "compare_function_clean")
149: Ret0
"#;
        assert_eq!(expect, actual);
    }

    #[test]
    fn print_regex() {
        let col_spec = new_spec("test");
        let test_doc = doc! {
            "name": doc! {
                "$regex": Regex {
                    options: String::new(),
                    pattern: "/^Vincent/".into(),
                },
            },
        };
        let program = SubProgram::compile_query(&col_spec, &test_doc, false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead("test")
5: Rewind(25)
10: Goto(55)

15: Label(3)
20: Next(55)

25: Label(6, "close")
30: Close
31: Halt

32: Label(5, "not_this_item")
37: Pop
38: Goto(15)

43: Label(4, "result")
48: ResultRow
49: Pop
50: Goto(15)

55: Label(2, "compare")
60: Dup
61: Call(80, 1)
70: FalseJump(32)
75: Goto(43)

80: Label(0, "compare_function")
85: GetField("name", 110)
94: PushValue(//^Vincent//)
99: Regex
100: FalseJump(110)
105: Pop2(2)

110: Label(1, "compare_function_clean")
115: Ret0
"#;
        assert_eq!(expect, actual);
    }

    #[test]
    fn print_update() {
        let col_spec = new_spec("test");
        let query_doc = doc! {
            "_id": doc! {
                "$gt": 3
            },
        };
        let update_doc = doc! {
            "$set": doc! {
                "name": "Alan Chan",
            },
            "$inc": doc! {
                "age": 1,
            },
            "$mul": doc! {
                "age": 3,
            },
            "$min": doc! {
                "age": 100,
            },
            "$unset": doc! {
                "age": "",
            },
            "$rename": doc! {
                "hello1": "hello2",
            },
        };
        let program =
            SubProgram::compile_update(&col_spec, &query_doc, &update_doc, false, true)
                .unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenWrite("test")
5: Rewind(25)
10: Goto(178)

15: Label(3)
20: Next(178)

25: Label(6, "close")
30: Close
31: Halt

32: Label(5, "not_this_item")
37: Pop
38: Goto(15)

43: Label(4, "result")
48: PushValue("Alan Chan")
53: SetField("name")
58: Pop
59: PushValue(1)
64: IncField("age")
69: Pop
70: PushValue(3)
75: MulField("age")
80: Pop
81: GetField("age", 136)
90: PushValue(100)
95: Less
96: FalseJump(106)
101: Goto(129)

106: Label(9)
111: Pop
112: Pop
113: PushValue(100)
118: SetField("age")
123: Pop
124: Goto(136)

129: Label(7)
134: Pop
135: Pop

136: Label(8)
141: UnsetField("age")
146: GetField("hello1", 166)
155: SetField("hello2")
160: Pop
161: UnsetField("hello1")

166: Label(10)
171: UpdateCurrent
172: Pop
173: Goto(15)

178: Label(2, "compare")
183: Dup
184: Call(203, 1)
193: FalseJump(32)
198: Goto(43)

203: Label(0, "compare_function")
208: GetField("_id", 233)
217: PushValue(3)
222: Greater
223: FalseJump(233)
228: Pop2(2)

233: Label(1, "compare_function_clean")
238: Ret0
"#;
        assert_eq!(expect, actual);
    }

    #[test]
    fn print_update_with_index() {
        let mut col_spec = new_spec("test");

        col_spec.indexes.insert(
            "age_1".into(),
            IndexInfo {
                keys: indexmap! {
                    "age".into() => 1,
                },
                options: None,
            },
        );

        let query_doc = doc! {
            "_id": {
                "$gt": 3
            },
        };
        let update_doc = doc! {
            "$set": {
                "name": "Alan Chan",
            },
        };
        let program =
            SubProgram::compile_update(&col_spec, &query_doc, &update_doc, false, true)
                .unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenWrite("test")
5: Rewind(25)
10: Goto(76)

15: Label(3)
20: Next(76)

25: Label(6, "close")
30: Close
31: Halt

32: Label(5, "not_this_item")
37: Pop
38: Goto(15)

43: Label(4, "result")
48: DeleteIndex("test")
53: PushValue("Alan Chan")
58: SetField("name")
63: Pop
64: UpdateCurrent
65: InsertIndex("test")
70: Pop
71: Goto(15)

76: Label(2, "compare")
81: Dup
82: Call(101, 1)
91: FalseJump(32)
96: Goto(43)

101: Label(0, "compare_function")
106: GetField("_id", 131)
115: PushValue(3)
120: Greater
121: FalseJump(131)
126: Pop2(2)

131: Label(1, "compare_function_clean")
136: Ret0
"#;
        assert_eq!(expect, actual);
    }

    #[test]
    fn test_aggregate_match() {
        let col_spec = new_spec("test");
        let program = SubProgram::compile_aggregate(&col_spec, vec![
            doc! {
                "$match": {
                    "age": {
                        "$gt": 18
                    },
                },
            },
        ], false).unwrap();
        let actual = format!("Program:\n\n{}", program);
        let expect = r#"Program:

0: OpenRead("test")
5: Rewind(25)
10: Goto(55)

15: Label(3)
20: Next(55)

25: Label(6, "close")
30: Close
31: Halt

32: Label(5, "not_this_item")
37: Pop
38: Goto(15)

43: Label(4, "result")
48: ResultRow
49: Pop
50: Goto(15)

55: Label(2, "compare")
60: Dup
61: Call(80, 1)
70: FalseJump(32)
75: Goto(43)

80: Label(0, "compare_function")
85: GetField("age", 110)
94: PushValue(18)
99: Greater
100: FalseJump(110)
105: Pop2(2)

110: Label(1, "compare_function_clean")
115: Ret0
"#;
        assert_eq!(expect, actual);
    }

    #[test]
    fn test_aggregate_count() {
        let col_spec = new_spec("test");
        let program = SubProgram::compile_aggregate(&col_spec, vec![
            doc! {
                "$match": {
                    "age": {
                        "$gt": 18
                    },
                },
            },
            doc! {
                "$count": "total",
            },
        ], false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead("test")
5: Rewind(25)
10: Goto(158)

15: Label(4)
20: Next(158)

25: Label(7, "close")
30: PushValue(null)
35: Call(76, 1)
44: Close
45: Halt

46: Label(6, "not_this_item")
51: Pop
52: Goto(15)

57: Label(5, "result")
62: Call(76, 1)
71: Goto(148)

76: Label(0)
81: Dup

82: Label(11)
87: CallExternal($count, 1)
96: TrueJump(103)
101: Pop
102: Ret0

103: Label(10)
108: Call(130, 1)
117: Pop
118: ExternalIsCompleted($count)
123: PushNull
124: FalseJump(82)
129: Ret0

130: Label(9, "final_result_row_fun")
135: EqualNull
136: TrueJump(142)
141: ResultRow

142: Label(12)
147: Ret0

148: Label(8, "next_item_label")
153: Goto(15)

158: Label(3, "compare")
163: Dup
164: Call(183, 1)
173: FalseJump(46)
178: Goto(57)

183: Label(1, "compare_function")
188: GetField("age", 213)
197: PushValue(18)
202: Greater
203: FalseJump(213)
208: Pop2(2)

213: Label(2, "compare_function_clean")
218: Ret0
"#;
        assert_eq!(expect, actual);
    }

    #[test]
    fn test_aggregate_count_without_match() {
        let col_spec = new_spec("test");
        let program = SubProgram::compile_aggregate(&col_spec, vec![
            doc! {
                "$count": "total",
            },
        ], false).unwrap();
        let actual = format!("Program:\n\n{}", program);
        let expect = r#"Program:

0: OpenRead("test")
5: Rewind(25)
10: Goto(46)

15: Label(1)
20: Next(46)

25: Label(2)
30: PushValue(null)
35: Call(65, 1)
44: Close
45: Halt

46: Label(0)
51: Call(65, 1)
60: Goto(137)

65: Label(3)
70: Dup

71: Label(7)
76: CallExternal($count, 1)
85: TrueJump(92)
90: Pop
91: Ret0

92: Label(6)
97: Call(119, 1)
106: Pop
107: ExternalIsCompleted($count)
112: PushNull
113: FalseJump(71)
118: Ret0

119: Label(5, "final_result_row_fun")
124: EqualNull
125: TrueJump(131)
130: ResultRow

131: Label(8)
136: Ret0

137: Label(4, "next_item_label")
142: Goto(15)
"#;
        assert_eq!(expect, actual);
    }
    #[test]
    fn test_aggregate_error_message() {
        let col_spec = new_spec("test");
        let program = SubProgram::compile_aggregate(&col_spec, vec![
            doc! {
                "$group": {
                    "_id": "$name",
                    "total": {
                        "$sumabc": 1,
                    },
                },
            },
        ], false);
        assert!(program.is_err());
        match program {
            Err(Error::InvalidField(i)) => {
                assert_eq!(i.path.unwrap().as_str(), "/0/$group/total/$sumabc");
            }
            _ => {
                panic!("Should return error");
            }
        }
    }
}
