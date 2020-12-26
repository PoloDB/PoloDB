use std::fmt;
use polodb_bson::{Value, Document};
use super::annotation::Annotation;
use crate::DbResult;
use crate::meta_doc_helper::{MetaDocEntry, meta_doc_key};
use super::op::DbOp;
use crate::vm::codegen::Codegen;

pub(crate) struct SubProgram {
    pub(super) static_values:    Vec<Value>,
    pub(super) instructions:     Vec<u8>,
    pub(super) annotation:       Option<Annotation>,
}

impl SubProgram {

    pub(super) fn new(annotation: bool) -> SubProgram {
        SubProgram {
            static_values: Vec::with_capacity(32),
            instructions: Vec::with_capacity(256),
            annotation: if annotation {
                Some(Annotation::new())
            } else {
                None
            }
        }
    }

    pub(crate) fn compile_query(entry: &MetaDocEntry, meta_doc: &Document, query: &Document, annotation: bool) -> DbResult<SubProgram> {
        let _indexes = meta_doc.get(meta_doc_key::INDEXES);
        // let _tuples = doc_to_tuples(doc);

        let mut codegen = Codegen::new(annotation);

        codegen.emit_open_read(entry.root_pid());

        codegen.emit_query_layout(query, |codegen| -> DbResult<()> {
            codegen.emit(DbOp::ResultRow);
            codegen.emit(DbOp::Pop);
            Ok(())
        })?;

        Ok(codegen.take())
    }

    pub(crate) fn compile_update(entry: &MetaDocEntry, query: Option<&Document>, update: &Document, annotation: bool) -> DbResult<SubProgram> {
        let mut codegen = Codegen::new(annotation);

        codegen.emit_open_write(entry.root_pid());

        codegen.emit_query_layout(query.unwrap(), |codegen| -> DbResult<()> {
            codegen.emit_update_operation(update)?;
            codegen.emit(DbOp::Pop);
            Ok(())
        })?;

        Ok(codegen.take())
    }

    pub(crate) fn compile_query_all(entry: &MetaDocEntry, annotation: bool) -> DbResult<SubProgram> {
        let mut codegen = Codegen::new(annotation);
        let result_label = codegen.new_label();
        let next_label = codegen.new_label();
        let close_label = codegen.new_label();

        codegen.emit_open_read(entry.root_pid());

        codegen.emit_goto(DbOp::Rewind, close_label);

        codegen.emit_goto(DbOp::Goto, result_label);

        codegen.emit_label(&next_label);
        codegen.emit_goto(DbOp::Next, result_label);

        codegen.emit_label(&close_label);
        codegen.emit(DbOp::Close);
        codegen.emit(DbOp::Halt);

        codegen.emit_label(&result_label);
        codegen.emit(DbOp::ResultRow);
        codegen.emit(DbOp::Pop);

        codegen.emit_goto(DbOp::Goto, next_label);

        Ok(codegen.take())
    }

    fn print_annotation(&self, f: &mut fmt::Formatter, pc: u32) -> fmt::Result {
        if let Some(annotation) = &self.annotation {
            return annotation.write_fmt(f, pc);
        }
        Ok(())
    }

}

impl fmt::Display for SubProgram {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        unsafe {
            let begin = self.instructions.as_ptr();
            let mut pc: usize = 0;
            while pc < self.instructions.len() {
                let op = begin.add(pc).cast::<DbOp>().read();
                self.print_annotation(f, pc as u32)?;
                match op {
                    DbOp::Goto => {
                        let location = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: Goto({})", pc, location)?;
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
    use polodb_bson::{mk_document, mk_array};
    use polodb_line_diff::assert_eq;
    use crate::vm::SubProgram;
    use crate::meta_doc_helper::MetaDocEntry;

    #[test]
    fn print_program() {
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let program = SubProgram::compile_query_all(&meta_entry, true).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = "Program:

0: OpenRead(100)
5: Rewind(20)
10: Goto(22)
15: Next(22)
20: Close
21: Halt
22: ResultRow
23: Pop
24: Goto(15)
";

        assert_eq!(expect, actual);
    }

    #[test]
    fn print_query() {
        let meta_doc = mk_document! {};
        let test_doc = mk_document! {
            "name": "Vincent Chan",
            "age": 32,
        };
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let program = SubProgram::compile_query(&meta_entry, &meta_doc, &test_doc, true).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead(100)
5: Rewind(20)
10: Goto(43)
15: Next(43)

Close:
20: Close
21: Halt

Not this item:
22: RecoverStackPos
23: Pop
24: Goto(15)

Get field failed:
29: RecoverStackPos
30: Pop
31: Goto(15)

Result:
36: ResultRow
37: Pop
38: Goto(15)

Compare:
43: SaveStackPos
44: GetField("name", 29)
53: PushValue("Vincent Chan")
58: Equal
59: FalseJump(22)
64: Pop
65: Pop
66: GetField("age", 29)
75: PushValue(32)
80: Equal
81: FalseJump(22)
86: Pop
87: Pop
88: Goto(36)
"#;
        assert_eq!(expect, actual)
    }

    #[test]
    fn print_query_by_primary_key() {
        let meta_doc = mk_document! {};
        let test_doc = mk_document! {
            "_id": 6,
            "age": 32,
        };
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let program = SubProgram::compile_query(&meta_entry, &meta_doc, &test_doc, true).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead(100)
5: PushValue(6)
10: FindByPrimaryKey(20)
15: Goto(23)
20: Pop
21: Close
22: Halt
23: GetField("age", 0)
32: PushValue(32)
37: Equal
38: FalseJump(20)
43: Pop
44: Pop
45: ResultRow
46: Pop
47: Goto(20)
"#;
        assert_eq!(expect, actual)
    }

    #[test]
    fn query_by_logic_and() {
        let meta_doc = mk_document! {};
        let test_doc = mk_document! {
            "$and": mk_array! [
                mk_document! {
                    "_id": 6,
                },
                mk_document! {
                    "age": 32,
                },
            ],
        };
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let program = SubProgram::compile_query(&meta_entry, &meta_doc, &test_doc, true).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

Program:

0: OpenRead(100)
5: Rewind(20)
10: Goto(43)
15: Next(43)

Close:
20: Close
21: Halt

Not this item:
22: RecoverStackPos
23: Pop
24: Goto(15)

Get field failed:
29: RecoverStackPos
30: Pop
31: Goto(15)

Result:
36: ResultRow
37: Pop
38: Goto(15)

Compare:
43: SaveStackPos
44: GetField("_id", 29)
53: PushValue(6)
58: Equal
59: FalseJump(22)
64: Pop
65: Pop
66: GetField("age", 29)
75: PushValue(32)
80: Equal
81: FalseJump(22)
86: Pop
87: Pop
88: Goto(36)
"#;
        assert_eq!(expect, actual)
    }

    #[test]
    fn print_logic_or() {
        let meta_doc = mk_document! {};
        let test_doc = mk_document! {
            "$or": mk_array! [
                mk_document! {
                    "age": 11,
                },
                mk_document! {
                    "age": 12,
                },
            ],
        };
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let program = SubProgram::compile_query(&meta_entry, &meta_doc, &test_doc, true).unwrap();
        let actual = format!("Program:\n\n{}", program);
        println!("{}", actual);
    }

    #[test]
    fn print_complex_print() {
        let meta_doc = mk_document! {};
        let test_doc = mk_document! {
            "age": mk_document! {
                "$gt": 3,
            },
            "child.age": mk_document! {
                "$in": mk_array! [ 1, 2 ],
            },
        };
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let program = SubProgram::compile_query(&meta_entry, &meta_doc, &test_doc, true).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenRead(100)
5: Rewind(20)
10: Goto(43)
15: Next(43)

Close:
20: Close
21: Halt

Not this item:
22: RecoverStackPos
23: Pop
24: Goto(15)

Get field failed:
29: RecoverStackPos
30: Pop
31: Goto(15)

Result:
36: ResultRow
37: Pop
38: Goto(15)

Compare:
43: SaveStackPos
44: GetField("age", 29)
53: PushValue(3)
58: Greater
59: FalseJump(22)
64: Pop2(2)
69: GetField("child", 29)
78: GetField("age", 29)
87: PushValue(Array(len=2))
92: In
93: FalseJump(22)
98: Pop2(3)
103: Goto(36)
"#;
        assert_eq!(expect, actual);
    }

    #[test]
    fn print_update() {
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let query_doc = mk_document! {
            "_id": mk_document! {
                "$gt": 3
            },
        };
        let update_doc = mk_document! {
            "$set": mk_document! {
                "name": "Alan Chan",
            },
            "$inc": mk_document! {
                "age": 1,
            },
            "$mul": mk_document! {
                "age": 3,
            },
            "$min": mk_document! {
                "age": 100,
            },
            "$unset": mk_document! {
                "age": "",
            },
            "$rename": mk_document! {
                "hello1": "hello2",
            },
        };
        let program = SubProgram::compile_update(&meta_entry, Some(&query_doc), &update_doc, true).unwrap();
        let actual = format!("Program:\n\n{}", program);

        let expect = r#"Program:

0: OpenWrite(100)
5: Rewind(20)
10: Goto(146)
15: Next(146)

Close:
20: Close
21: Halt

Not this item:
22: RecoverStackPos
23: Pop
24: Goto(15)

Get field failed:
29: RecoverStackPos
30: Pop
31: Goto(15)

Result:
36: PushValue("Alan Chan")
41: SetField("name")
46: Pop
47: PushValue(1)
52: IncField("age")
57: Pop
58: PushValue(3)
63: MulField("age")
68: Pop
69: GetField("age", 114)
78: PushValue(100)
83: Less
84: FalseJump(94)
89: Goto(112)
94: Pop
95: Pop
96: PushValue(100)
101: SetField("age")
106: Pop
107: Goto(114)
112: Pop
113: Pop
114: UnsetField("age")
119: GetField("hello1", 139)
128: SetField("hello2")
133: Pop
134: UnsetField("hello1")
139: UpdateCurrent
140: Pop
141: Goto(15)

Compare:
146: SaveStackPos
147: GetField("_id", 29)
156: PushValue(3)
161: Greater
162: FalseJump(22)
167: Pop2(2)
172: Goto(36)
"#;
        assert_eq!(expect, actual);
    }

}
