use std::fmt;
use polodb_bson::{Value, Document};
use crate::DbResult;
use crate::meta_doc_helper::{MetaDocEntry, meta_doc_key};
use super::op::DbOp;
use crate::vm::codegen::Codegen;

pub(crate) struct SubProgram {
    pub(super) static_values:    Vec<Value>,
    pub(super) instructions:     Vec<u8>,
}

impl SubProgram {

    pub(super) fn new() -> SubProgram {
        SubProgram {
            static_values: Vec::with_capacity(16),
            instructions: Vec::with_capacity(64),
        }
    }

    pub(crate) fn compile_query(entry: &MetaDocEntry, meta_doc: &Document, query: &Document) -> DbResult<SubProgram> {
        let _indexes = meta_doc.get(meta_doc_key::INDEXES);
        // let _tuples = doc_to_tuples(doc);

        let mut codegen = Codegen::new();

        codegen.emit_open_read(entry.root_pid());

        codegen.emit_query_layout(query, |codegen| -> DbResult<()> {
            codegen.emit(DbOp::ResultRow);
            codegen.emit(DbOp::Pop);
            Ok(())
        })?;

        Ok(codegen.take())
    }

    pub(crate) fn compile_update(entry: &MetaDocEntry, query: Option<&Document>, update: &Document) -> DbResult<SubProgram> {
        let mut codegen = Codegen::new();

        codegen.emit_open_write(entry.root_pid());

        codegen.emit_query_layout(query.unwrap(), |codegen| -> DbResult<()> {
            codegen.emit_update_operation(update)?;
            codegen.emit(DbOp::Pop);
            Ok(())
        })?;

        Ok(codegen.take())
    }

    pub(crate) fn compile_query_all(entry: &MetaDocEntry) -> DbResult<SubProgram> {
        let mut codegen = Codegen::new();

        codegen.emit_open_read(entry.root_pid());

        let rewind_loc = codegen.current_location();
        codegen.emit(DbOp::Rewind);
        codegen.emit_u32(0);

        let goto_loc = codegen.current_location();
        codegen.emit_goto(0);

        let location = codegen.current_location();
        codegen.emit_next(0);

        let close_loc = codegen.current_location();
        codegen.emit(DbOp::Close);
        codegen.emit(DbOp::Halt);

        let result_location = codegen.current_location();
        codegen.update_next_location(location as usize, result_location);

        let result_loc = codegen.current_location();
        codegen.emit(DbOp::ResultRow);
        codegen.emit(DbOp::Pop);

        codegen.update_next_location(goto_loc as usize, result_loc);

        codegen.emit_goto(location);

        codegen.update_next_location(rewind_loc as usize, close_loc);

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

                    DbOp::IfGreater => {
                        let location = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: IfGreater({})", pc, location)?;
                        pc += 5;
                    }

                    DbOp::IfLess => {
                        let location = begin.add(pc + 1).cast::<u32>().read();
                        writeln!(f, "{}: IfLess({})", pc, location)?;
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

                    DbOp::Cmp => {
                        writeln!(f, "{}: Cmp", pc)?;
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
    use crate::vm::SubProgram;
    use crate::meta_doc_helper::MetaDocEntry;

    #[test]
    fn print_program() {
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let program = SubProgram::compile_query_all(&meta_entry).unwrap();
        println!("Program: \n\n{}", program);
    }

    #[test]
    fn print_query() {
        let meta_doc = mk_document! {};
        let test_doc = mk_document! {
            "name": "Vincent Chan",
            "age": 32,
        };
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let program = SubProgram::compile_query(&meta_entry, &meta_doc, &test_doc).unwrap();
        println!("Program: \n\n{}", program);
    }

    #[test]
    fn print_query_by_primary_key() {
        let meta_doc = mk_document! {};
        let test_doc = mk_document! {
            "_id": 6,
            "age": 32,
        };
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let program = SubProgram::compile_query(&meta_entry, &meta_doc, &test_doc).unwrap();
        println!("Program: \n\n{}", program);
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
        let program = SubProgram::compile_query(&meta_entry, &meta_doc, &test_doc).unwrap();
        println!("Program: \n\n{}", program);
    }

    #[test]
    fn print_update() {
        let meta_entry = MetaDocEntry::new(0, "test".into(), 100);
        let query_doc = mk_document! {
            "name": "Vincent Chan",
            "age": 32,
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
        let program = SubProgram::compile_update(&meta_entry, Some(&query_doc), &update_doc).unwrap();
        println!("Program: \n\n{}", program);
    }

}
