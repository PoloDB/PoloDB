use bson::{Bson, Document};
use crate::vm::update_operators::{UpdateOperator, UpdateResult};
use crate::Result;

pub(crate) struct SetOperator {
    doc: Document
}

impl SetOperator {

    pub fn compile(doc: Document) -> Result<SetOperator> {
        <dyn UpdateOperator>::validate_key(&doc)?;
        Ok(SetOperator {
            doc
        })
    }

}

impl UpdateOperator for SetOperator {

    fn name(&self) -> &str {
        "set"
    }

    fn update(&self, value: &mut Bson) -> Result<UpdateResult> {
        let doc = value.as_document_mut().unwrap();

        let mut updated = false;
        for (k, v) in self.doc.iter() {
            doc.insert(k.clone(), v.clone());
            updated = true;
        }

        Ok(UpdateResult {
            updated,
        })
    }

}
