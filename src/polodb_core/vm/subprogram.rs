use std::fmt;
use bson::{Bson, Document};
use crate::DbResult;
use crate::meta_doc_helper::{MetaDocEntry, meta_doc_key};
use super::op::DbOp;
use super::label::LabelSlot;
use crate::vm::codegen::Codegen;

pub(crate) struct SubProgram {
    pub(super) static_values:    Vec<Bson>,
    pub(super) instructions:     Vec<u8>,
    pub(super) label_slots:      Vec<LabelSlot>,
}

impl SubProgram {

    pub(super) fn new() -> SubProgram {
        SubProgram {
            static_values: Vec::with_capacity(32),
            instructions: Vec::with_capacity(256),
            label_slots: Vec::with_capacity(32),
        }
    }

    pub(crate) fn compile_query(entry: &MetaDocEntry, meta_doc: &Document, query: &Document,
                                skip_annotation: bool) -> DbResult<SubProgram> {
        let _indexes = meta_doc.get(meta_doc_key::INDEXES);
        // let _tuples = doc_to_tuples(doc);

        let mut codegen = Codegen::new(skip_annotation);

        codegen.emit_open_read(entry.root_pid());

        codegen.emit_query_layout(
            query,
            |codegen| -> DbResult<()> {
                codegen.emit(DbOp::ResultRow);
                codegen.emit(DbOp::Pop);
                Ok(())
            },
            true
        )?;

        Ok(codegen.take())
    }

    pub(crate) fn compile_update(entry: &MetaDocEntry, query: Option<&Document>, update: &Document,
                                 skip_annotation: bool, is_many: bool) -> DbResult<SubProgram> {
        let mut codegen = Codegen::new(skip_annotation);

        codegen.emit_open_write(entry.root_pid());

        codegen.emit_query_layout(
            query.unwrap(),
            |codegen| -> DbResult<()> {
                codegen.emit_update_operation(update)?;
                codegen.emit(DbOp::Pop);
                codegen.emit(DbOp::IncR2);
                Ok(())
            },
            is_many
        )?;

        Ok(codegen.take())
    }

    pub(crate) fn compile_query_all(entry: &MetaDocEntry, skip_annotation: bool) -> DbResult<SubProgram> {
        let mut codegen = Codegen::new(skip_annotation);
        let result_label = codegen.new_label();
        let next_label = codegen.new_label();
        let close_label = codegen.new_label();

        codegen.emit_open_read(entry.root_pid());

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
                        let root_pid = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: OpenRead({})", pc, root_pid)?;
                        pc += 5;
                    }

                    DbOp::OpenWrite => {
                        let root_pid = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: OpenWrite({})", pc, root_pid)?;
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
    use polodb_line_diff::assert_eq;
    use crate::vm::SubProgram;
    use crate::meta_doc_helper::MetaDocEntry;

    #[test]
    fn print_program() {
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let program = SubProgram::compile_query_all(&meta_entry, false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = "Program:

0: OpenRead(100)
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
        let meta_doc = doc! {};
        let test_doc = doc! {
            "name": "Vincent Chan",
            "age": 32,
        };
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let program = SubProgram::compile_query(&meta_entry, &meta_doc, &test_doc, false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead(100)
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
        let meta_doc = doc! {};
        let test_doc = doc! {
            "_id": 6,
            "age": 32,
        };
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let program = SubProgram::compile_query(&meta_entry, &meta_doc, &test_doc, false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead(100)
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
    fn query_by_logic_and() {
        let meta_doc = doc! {};
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
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let program = SubProgram::compile_query(&meta_entry, &meta_doc, &test_doc, false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead(100)
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
        let meta_doc = doc!();
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
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let program = SubProgram::compile_query(&meta_entry, &meta_doc, &test_doc, false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead(100)
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
        let meta_doc = doc! {};
        let test_doc = doc! {
            "age": doc! {
                "$gt": 3,
            },
            "child.age": doc! {
                "$in": [ 1, 2 ],
            },
        };
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let program = SubProgram::compile_query(&meta_entry, &meta_doc, &test_doc, false).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead(100)
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
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
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
        let program = SubProgram::compile_update(&meta_entry,
                                                 Some(&query_doc), &update_doc,
                                                 false, true).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenWrite(100)
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

}
