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

    #[allow(dead_code)]
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
