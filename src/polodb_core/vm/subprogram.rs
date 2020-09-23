use std::fmt;
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

    pub(crate) fn compile_query(entry: &MetaDocEntry, meta_doc: &Document, doc: &Document) -> DbResult<SubProgram> {
        let _indexes = meta_doc.get(meta_doc_key::INDEXES);
        let _tuples = doc_to_tuples(doc);

        let mut result = SubProgram::new();
        result.add_open_read(entry.root_pid);

        let current_loc = result.current_location();
        result.add_next(current_loc);

        result.add(DbOp::ResultRow);

        result.add(DbOp::Close);
        result.add(DbOp::Halt);

        Ok(result)
    }

    pub(crate) fn compile_update(meta_doc: &Document, _query: &Document, _update: &Document) -> DbResult<SubProgram> {
        unimplemented!()
    }

    pub(crate) fn compile_query_all(entry: &MetaDocEntry) -> DbResult<SubProgram> {
        let mut result = SubProgram::new();

        result.add_open_read(entry.root_pid);
        result.add(DbOp::Rewind);

        let location = result.instructions.len() as u32;
        result.add_next(0);

        result.add(DbOp::Close);
        result.add(DbOp::Halt);

        let result_location = result.instructions.len() as u32;
        result.update_next_location(location as usize, result_location);
        result.add(DbOp::ResultRow);
        result.add(DbOp::Pop);

        result.add_goto(location);

        Ok(result)
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

    #[inline]
    fn add(&mut self, op: DbOp) {
        self.instructions.push(op as u8);
    }

    #[inline]
    fn current_location(&self) -> u32 {
        self.instructions.len() as u32
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
                        write!(f, "{}: PushValue({})\n", pc, index)?;
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
    use crate::vm::SubProgram;
    use crate::meta_doc_helper::MetaDocEntry;

    #[test]
    fn print_program() {
        let meta_entry = MetaDocEntry::new("test".into(), 100);
        let program = SubProgram::compile_query_all(&meta_entry).unwrap();
        println!("Program: \n\n{}", program);
    }

}
