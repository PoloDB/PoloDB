/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::fmt;
use bson::{Bson, Document};
use indexmap::IndexMap;
use crate::coll::collection_info::{
    CollectionSpecification,
    IndexInfo,
};
use crate::Result;
use super::op::DbOp;
use super::label::LabelSlot;
use crate::vm::codegen::Codegen;

pub(crate) struct SubProgramIndexItem {
    pub col_name: String,
    pub indexes: IndexMap<String, IndexInfo>,
}

pub(crate) struct SubProgram {
    pub(super) static_values: Vec<Bson>,
    pub(super) instructions:  Vec<u8>,
    pub(super) label_slots:   Vec<LabelSlot>,
    pub(super) index_infos:   Vec<SubProgramIndexItem>,
}

impl SubProgram {

    pub(super) fn new() -> SubProgram {
        SubProgram {
            static_values: Vec::with_capacity(32),
            instructions: Vec::with_capacity(256),
            label_slots: Vec::with_capacity(32),
            index_infos: Vec::new(),
        }
    }

    pub(crate) fn compile_empty_query() -> SubProgram {
        let mut codegen = Codegen::new(true);

        codegen.emit(DbOp::Halt);

        codegen.take()
    }

    pub(crate) fn compile_query(
        col_spec: &CollectionSpecification,
        query: &Document,
        skip_annotation: bool,
    ) -> Result<SubProgram> {
        let mut codegen = Codegen::new(skip_annotation);

        codegen.emit_open_read(col_spec._id.clone().into());

        codegen.emit_query_layout(
            col_spec,
            query,
            |codegen| -> Result<()> {
                codegen.emit(DbOp::ResultRow);
                codegen.emit(DbOp::Pop);
                Ok(())
            },
            true
        )?;

        Ok(codegen.take())
    }

    pub(crate) fn compile_update(
        col_spec: &CollectionSpecification,
        query: Option<&Document>,
        update: &Document,
        skip_annotation: bool, is_many: bool,
    ) -> Result<SubProgram> {
        let mut codegen = Codegen::new(skip_annotation);

        let has_indexes = !col_spec.indexes.is_empty();
        let index_item_id: u32 = if has_indexes {
            codegen.push_index_info(SubProgramIndexItem {
                col_name: col_spec._id.to_string(),
                indexes: col_spec.indexes.clone()
            })
        } else {
            u32::MAX
        };

        codegen.emit_open_write(col_spec._id.clone().into());

        codegen.emit_query_layout(
            col_spec,
            query.unwrap(),
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
            is_many
        )?;

        Ok(codegen.take())
    }

    pub(crate) fn compile_delete(
        col_spec: &CollectionSpecification,
        col_name: &str,
        query: Option<&Document>,
        skip_annotation: bool, is_many: bool,
    ) -> Result<SubProgram> {
        let mut codegen = Codegen::new(skip_annotation);

        codegen.emit_open_write(col_name.into());

        codegen.emit_query_layout(
            col_spec,
            query.unwrap(),
            |codegen| -> Result<()> {
                codegen.emit_delete_operation();
                codegen.emit(DbOp::Pop);
                Ok(())
            },
            is_many
        )?;

        Ok(codegen.take())
    }

    // TODO: need test
    pub(crate) fn compile_delete_all(
        col_name: &str,
        skip_annotation: bool
    ) -> Result<SubProgram> {
        let mut codegen = Codegen::new(skip_annotation);
        let result_label = codegen.new_label();
        let next_label = codegen.new_label();
        let close_label = codegen.new_label();

        codegen.emit_open_read(col_name.into());

        codegen.emit_goto(DbOp::Rewind, close_label);

        codegen.emit_goto(DbOp::Goto, result_label);

        codegen.emit_label(next_label);
        codegen.emit_goto(DbOp::Next, result_label);

        codegen.emit_label(close_label);
        codegen.emit(DbOp::Close);
        codegen.emit(DbOp::Halt);

        codegen.emit_label(result_label);
        codegen.emit_delete_operation();
        codegen.emit(DbOp::Pop);

        codegen.emit_goto(DbOp::Goto, next_label);

        Ok(codegen.take())
    }

    pub(crate) fn compile_query_all(col_spec: &CollectionSpecification, skip_annotation: bool) -> Result<SubProgram> {
        SubProgram::compile_query_all_by_name(col_spec.name(), skip_annotation)
    }

    pub(crate) fn compile_query_all_by_name(col_name: &str, skip_annotation: bool) -> Result<SubProgram> {
        let mut codegen = Codegen::new(skip_annotation);
        let result_label = codegen.new_label();
        let next_label = codegen.new_label();
        let close_label = codegen.new_label();

        codegen.emit_open_read(col_name.into());

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

}

impl fmt::Display for SubProgram {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
                            LabelSlot::UnnamedLabel(_) =>
                                writeln!(f, "{}: Label({})", pc, label_id)?,
                            LabelSlot::LabelWithString(_, name) =>
                                writeln!(f, "{}: Label({}, \"{}\")", pc, label_id, name)?,
                        }
                        pc += 5;
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

                    DbOp::PushValue => {
                        let index = begin.add(pc + 1).cast::<u32>().read();
                        let val = &self.static_values[index as usize];
                        writeln!(f, "{}: PushValue({})", pc, val)?;
                        pc += 5;
                    }

                    DbOp::PushR0 => {
                        writeln!(f, "{}: PushR0", pc)?;
                        pc += 1;
                    }

                    DbOp::StoreR0 => {
                        writeln!(f, "{}: StoreR0", pc)?;
                        pc += 1;
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

                    DbOp::In => {
                        writeln!(f, "{}: In", pc)?;
                        pc += 1;
                    }

                    DbOp::OpenRead => {
                        let idx = begin.add(pc + 1).cast::<u32>().read();
                        let value = &self.static_values[idx as usize];
                        writeln!(f, "{}: OpenRead({})", pc, value)?;
                        pc += 5;
                    }

                    DbOp::OpenWrite => {
                        let idx = begin.add(pc + 1).cast::<u32>().read();
                        let value = &self.static_values[idx as usize];
                        writeln!(f, "{}: OpenWrite({})", pc, value)?;
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

                    DbOp::SaveStackPos => {
                        writeln!(f, "{}: SaveStackPos", pc)?;
                        pc += 1;
                    }

                    DbOp::RecoverStackPos => {
                        writeln!(f, "{}: RecoverStackPos", pc)?;
                        pc += 1;
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
    use bson::doc;
    use indexmap::indexmap;
    use polodb_line_diff::assert_eq;
    use crate::coll::collection_info::{CollectionSpecification, IndexInfo};
    use crate::vm::SubProgram;

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
5: Rewind(30)
10: Goto(37)

15: Label(1)
20: Next(37)

25: Label(2)
30: Close
31: Halt

32: Label(0)
37: ResultRow
38: Pop
39: Goto(20)
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
5: Rewind(30)
10: Goto(73)

15: Label(1)
20: Next(73)

25: Label(5, "Close")
30: Close
31: Halt

32: Label(4, "Not this item")
37: RecoverStackPos
38: Pop
39: Goto(20)

44: Label(3, "Get field failed")
49: RecoverStackPos
50: Pop
51: Goto(20)

56: Label(2, "Result")
61: ResultRow
62: Pop
63: Goto(20)

68: Label(0, "Compare")
73: SaveStackPos
74: GetField("name", 49)
83: PushValue("Vincent Chan")
88: Equal
89: FalseJump(37)
94: Pop
95: Pop
96: GetField("age", 49)
105: PushValue(32)
110: Equal
111: FalseJump(37)
116: Pop
117: Pop
118: Goto(61)
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
10: FindByPrimaryKey(25)
15: Goto(33)

20: Label(0)
25: Pop
26: Close
27: Halt

28: Label(1)
33: GetField("age", 25)
42: PushValue(32)
47: Equal
48: FalseJump(25)
53: Pop
54: Pop
55: ResultRow
56: Pop
57: Goto(25)
"#;
        assert_eq!(expect, actual)
    }

    #[test]
    fn print_query_by_index() {
        let mut col_spec = new_spec("test");

        col_spec.indexes.insert("age_1".into(), IndexInfo {
            keys: indexmap! {
                "age".into() => 1,
            },
            options: None,
        });

        let test_doc = doc! {
            "age": 32,
            "name": "Vincent Chan",
        };

        let program = SubProgram::compile_query(&col_spec, &test_doc, false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead("test")
5: PushValue(32)
10: PushValue("age_1")
15: PushValue("test")
20: FindByIndex(35)
25: Goto(45)

30: Label(0)
35: Pop
36: Pop
37: Pop
38: Close
39: Halt

40: Label(1)
45: GetField("name", 35)
54: PushValue("Vincent Chan")
59: Equal
60: FalseJump(35)
65: Pop
66: Pop
67: ResultRow
68: Pop
69: Goto(35)
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
5: Rewind(30)
10: Goto(73)

15: Label(1)
20: Next(73)

25: Label(5, "Close")
30: Close
31: Halt

32: Label(4, "Not this item")
37: RecoverStackPos
38: Pop
39: Goto(20)

44: Label(3, "Get field failed")
49: RecoverStackPos
50: Pop
51: Goto(20)

56: Label(2, "Result")
61: ResultRow
62: Pop
63: Goto(20)

68: Label(0, "Compare")
73: SaveStackPos
74: GetField("_id", 49)
83: PushValue(6)
88: Equal
89: FalseJump(37)
94: Pop
95: Pop
96: GetField("age", 49)
105: PushValue(32)
110: Equal
111: FalseJump(37)
116: Pop
117: Pop
118: Goto(61)
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
5: Rewind(30)
10: Goto(73)

15: Label(1)
20: Next(73)

25: Label(5, "Close")
30: Close
31: Halt

32: Label(4, "Not this item")
37: RecoverStackPos
38: Pop
39: Goto(20)

44: Label(3, "Get field failed")
49: RecoverStackPos
50: Pop
51: Goto(20)

56: Label(2, "Result")
61: ResultRow
62: Pop
63: Goto(20)

68: Label(0, "Compare")
73: SaveStackPos
74: Goto(95)

79: Label(7)
84: RecoverStackPos
85: Goto(127)

90: Label(8)
95: GetField("age", 84)
104: PushValue(11)
109: Equal
110: FalseJump(84)
115: Pop
116: Pop
117: Goto(61)

122: Label(6)
127: GetField("age", 49)
136: PushValue(12)
141: Equal
142: FalseJump(37)
147: Pop
148: Pop
149: Goto(61)
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
5: Rewind(30)
10: Goto(73)

15: Label(1)
20: Next(73)

25: Label(5, "Close")
30: Close
31: Halt

32: Label(4, "Not this item")
37: RecoverStackPos
38: Pop
39: Goto(20)

44: Label(3, "Get field failed")
49: RecoverStackPos
50: Pop
51: Goto(20)

56: Label(2, "Result")
61: ResultRow
62: Pop
63: Goto(20)

68: Label(0, "Compare")
73: SaveStackPos
74: GetField("age", 49)
83: PushValue(3)
88: Greater
89: FalseJump(37)
94: Pop2(2)
99: GetField("child", 49)
108: GetField("age", 49)
117: PushValue([1, 2])
122: In
123: FalseJump(37)
128: Pop2(3)
133: Goto(61)
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
        let program = SubProgram::compile_update(
            &col_spec,
            Some(&query_doc),
            &update_doc,
            false, true
        ).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenWrite("test")
5: Rewind(30)
10: Goto(196)

15: Label(1)
20: Next(196)

25: Label(5, "Close")
30: Close
31: Halt

32: Label(4, "Not this item")
37: RecoverStackPos
38: Pop
39: Goto(20)

44: Label(3, "Get field failed")
49: RecoverStackPos
50: Pop
51: Goto(20)

56: Label(2, "Result")
61: PushValue("Alan Chan")
66: SetField("name")
71: Pop
72: PushValue(1)
77: IncField("age")
82: Pop
83: PushValue(3)
88: MulField("age")
93: Pop
94: GetField("age", 154)
103: PushValue(100)
108: Less
109: FalseJump(124)
114: Goto(147)

119: Label(8)
124: Pop
125: Pop
126: PushValue(100)
131: SetField("age")
136: Pop
137: Goto(154)

142: Label(6)
147: Pop
148: Pop

149: Label(7)
154: UnsetField("age")
159: GetField("hello1", 184)
168: SetField("hello2")
173: Pop
174: UnsetField("hello1")

179: Label(9)
184: UpdateCurrent
185: Pop
186: Goto(20)

191: Label(0, "Compare")
196: SaveStackPos
197: GetField("_id", 49)
206: PushValue(3)
211: Greater
212: FalseJump(37)
217: Pop2(2)
222: Goto(61)
"#;
        assert_eq!(expect, actual);
    }

    #[test]
    fn print_update_with_index() {
        let mut col_spec = new_spec("test");

        col_spec.indexes.insert("age_1".into(), IndexInfo {
            keys: indexmap! {
                "age".into() => 1,
            },
            options: None,
        });

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
        let program = SubProgram::compile_update(
            &col_spec,
            Some(&query_doc),
            &update_doc,
            false, true
        ).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenWrite("test")
5: Rewind(30)
10: Goto(94)

15: Label(1)
20: Next(94)

25: Label(5, "Close")
30: Close
31: Halt

32: Label(4, "Not this item")
37: RecoverStackPos
38: Pop
39: Goto(20)

44: Label(3, "Get field failed")
49: RecoverStackPos
50: Pop
51: Goto(20)

56: Label(2, "Result")
61: DeleteIndex("test")
66: PushValue("Alan Chan")
71: SetField("name")
76: Pop
77: UpdateCurrent
78: InsertIndex("test")
83: Pop
84: Goto(20)

89: Label(0, "Compare")
94: SaveStackPos
95: GetField("_id", 49)
104: PushValue(3)
109: Greater
110: FalseJump(37)
115: Pop2(2)
120: Goto(61)
"#;
        assert_eq!(expect, actual);
    }

}
