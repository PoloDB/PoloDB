use std::fmt;
use std::rc::Rc;
use crate::bson::{Value, Document};
use crate::DbResult;
use crate::meta_doc_helper::{MetaDocEntry, meta_doc_key};
use super::op::DbOp;

pub(crate) struct SubProgram {
    pub(super) static_values:    Vec<Value>,
    pub(super) instructions:     Vec<u8>,
}

fn doc_to_tuples(doc: &Document) -> Vec<(String, Value)> {
    let mut result = Vec::with_capacity(doc.len());
    for (key, value) in doc.iter() {
        result.push((key.clone(), value.clone()))
    }
    result
}

impl SubProgram {

    fn new() -> SubProgram {
        SubProgram {
            static_values: Vec::with_capacity(16),
            instructions: Vec::with_capacity(64),
        }
    }

    pub(crate) fn compile_query(entry: &MetaDocEntry, meta_doc: &Document, query: &Document) -> DbResult<SubProgram> {
        let _indexes = meta_doc.get(meta_doc_key::INDEXES);
        // let _tuples = doc_to_tuples(doc);

        let mut program = SubProgram::new();
        program.add_open_read(entry.root_pid);
        program.add(DbOp::Rewind);

        let next_preserve_location = program.current_location();
        program.add_next(0);

        program.add(DbOp::Close);
        program.add(DbOp::Halt);

        // let result_location = program.current_location();

        let not_found_branch_preserve_location = program.current_location();
        program.add(DbOp::Pop);
        program.add(DbOp::Pop);
        program.add(DbOp::Pop);  // pop the current value;
        program.add_goto(next_preserve_location);

        let get_field_failed_location = program.current_location();
        program.add(DbOp::Pop);
        program.add_goto(next_preserve_location);

        let compare_location: u32 = program.current_location();

        for (key, value) in query.iter() {
            let key_static_id = program.push_static(Value::String(Rc::new(key.clone())));
            let value_static_id = program.push_static(value.clone());

            program.add_get_field(key_static_id, get_field_failed_location);  // push a value1
            program.add_push_value(value_static_id);  // push a value2

            program.add(DbOp::Equal);
            // if not equal，go to next
            program.add_false_jump(not_found_branch_preserve_location);

            program.add(DbOp::Pop); // pop a value2
            program.add(DbOp::Pop); // pop a value1
        }

        program.update_next_location(next_preserve_location as usize, compare_location);

        program.add(DbOp::ResultRow);

        program.add(DbOp::Pop);

        program.add_goto(next_preserve_location);

        Ok(program)
    }

    pub(crate) fn compile_update(meta_doc: &Document, _query: &Document, _update: &Document) -> DbResult<SubProgram> {
        unimplemented!()
    }

    pub(crate) fn compile_query_all(entry: &MetaDocEntry) -> DbResult<SubProgram> {
        let mut program = SubProgram::new();

        program.add_open_read(entry.root_pid);
        program.add(DbOp::Rewind);

        let location = program.current_location();
        program.add_next(0);

        program.add(DbOp::Close);
        program.add(DbOp::Halt);

        let result_location = program.instructions.len() as u32;
        program.update_next_location(location as usize, result_location);

        program.add(DbOp::ResultRow);
        program.add(DbOp::Pop);

        program.add_goto(location);

        Ok(program)
    }

    #[inline]
    fn update_next_location(&mut self, pos: usize, location: u32) {
        let loc_be = location.to_le_bytes();
        self.instructions[pos + 1..pos + 5].copy_from_slice(&loc_be);
    }

    fn add_goto(&mut self, location: u32) {
        self.add(DbOp::Goto);
        let bytes = location.to_le_bytes();
        self.instructions.extend_from_slice(&bytes);
    }

    fn add_open_read(&mut self, root_pid: u32) {
        self.add(DbOp::OpenRead);
        let bytes = root_pid.to_le_bytes();
        self.instructions.extend_from_slice(&bytes);
    }

    fn add_next(&mut self, location: u32) {
        self.add(DbOp::Next);
        let bytes = location.to_le_bytes();
        self.instructions.extend_from_slice(&bytes);
    }

    fn add_push_value(&mut self, static_id: u32) {
        self.add(DbOp::PushValue);
        let bytes = static_id.to_le_bytes();
        self.instructions.extend_from_slice(&bytes);
    }

    fn add_false_jump(&mut self, location: u32) {
        self.add(DbOp::FalseJump);
        let bytes = location.to_le_bytes();
        self.instructions.extend_from_slice(&bytes);
    }

    fn add_get_field(&mut self, static_id: u32, failed_location: u32) {
        self.add(DbOp::GetField);
        let bytes = static_id.to_le_bytes();
        self.instructions.extend_from_slice(&bytes);
        let bytes = failed_location.to_le_bytes();
        self.instructions.extend_from_slice(&bytes);
    }

    #[inline]
    fn add(&mut self, op: DbOp) {
        self.instructions.push(op as u8);
    }

    #[inline]
    fn current_location(&self) -> u32 {
        self.instructions.len() as u32
    }

    #[inline]
    fn push_static(&mut self, value: Value) -> u32 {
        let pos = self.static_values.len() as u32;
        self.static_values.push(value);
        pos
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
                        write!(f, "{}: Goto({})\n", pc, location)?;
                        pc += 5;
                    }

                    DbOp::TrueJump => {
                        let location = begin.add(pc + 1).cast::<u32>().read();
                        write!(f, "{}: TrueJump({})\n", pc, location)?;
                        pc += 5;
                    }

                    DbOp::FalseJump => {
                        let location = begin.add(pc + 1).cast::<u32>().read();
                        write!(f, "{}: FalseJump({})\n", pc, location)?;
                        pc += 5;
                    }

                    DbOp::Rewind => {
                        write!(f, "{}: Rewind\n", pc)?;
                        pc += 1;
                    }

                    DbOp::Next => {
                        let location = begin.add(pc + 1).cast::<u32>().read();
                        write!(f, "{}: Next({})\n", pc, location)?;
                        pc += 5;
                    }

                    DbOp::PushValue => {
                        let index = begin.add(pc + 1).cast::<u32>().read();
                        let val = &self.static_values[index as usize];
                        write!(f, "{}: PushValue({})\n", pc, val)?;
                        pc += 5;
                    }

                    DbOp::Pop => {
                        write!(f, "{}: Pop\n", pc)?;
                        pc += 1;
                    }

                    DbOp::Equal => {
                        write!(f, "{}: Equal\n", pc)?;
                        pc += 1;
                    }

                    DbOp::Cmp => {
                        write!(f, "{}: Cmp\n", pc)?;
                        pc += 1;
                    }

                    DbOp::OpenRead => {
                        let root_pid = begin.add(pc + 1).cast::<u32>().read();
                        write!(f, "{}: OpenRead({})\n", pc, root_pid)?;
                        pc += 5;
                    }

                    DbOp::ResultRow => {
                        write!(f, "{}: ResultRow\n", pc)?;
                        pc += 1;
                    }

                    DbOp::Close => {
                        write!(f, "{}: Close\n", pc)?;
                        pc += 1;
                    }

                    DbOp::Halt => {
                        write!(f, "{}: Halt\n", pc)?;
                        pc += 1;
                    }

                    DbOp::GetField => {
                        let static_id = begin.add(pc + 1).cast::<u32>().read();
                        let val = &self.static_values[static_id as usize];
                        let location = begin.add(pc + 5).cast::<u32>().read();
                        write!(f, "{}: GetField({}, {})\n", pc, val, location)?;
                        pc += 9;
                    }

                    _ => {
                        write!(f, "{}: Unknown\n", pc)?;
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
    use std::rc::Rc;
    use crate::vm::SubProgram;
    use crate::meta_doc_helper::MetaDocEntry;
    use crate::bson::{Document, Value};

    #[test]
    fn print_program() {
        let meta_entry = MetaDocEntry::new("test".into(), 100);
        let program = SubProgram::compile_query_all(&meta_entry).unwrap();
        println!("Program: \n\n{}", program);
    }

    #[test]
    fn print_query() {
        let mut meta_doc = Document::new_without_id();
        let mut test_doc = Document::new_without_id();
        test_doc.insert("name".into(), Value::String(Rc::new("Vincent Chan".into())));
        test_doc.insert("age".into(), Value::Int(32));
        let meta_entry = MetaDocEntry::new("test".into(), 100);
        let program = SubProgram::compile_query(&meta_entry, &meta_doc, &test_doc).unwrap();
        println!("Program: \n\n{}", program);
    }

}
