use crate::bson::{Value, Document};
use crate::DbResult;

pub(crate) struct SubProgram {
    pub(super) static_values:    Vec<Value>,
    pub(super) instructions:     Vec<u8>,
}

impl SubProgram {

    pub(crate) fn compile_query(_doc: &Document) -> DbResult<SubProgram> {
        unimplemented!()
    }

    pub(crate) fn compile_update(_query: &Document, _update: &Document) -> DbResult<SubProgram> {
        unimplemented!()
    }

}
