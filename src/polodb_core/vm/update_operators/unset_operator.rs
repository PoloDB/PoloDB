use bson::{Bson, Document};
use crate::Result;
use crate::vm::update_operators::{UpdateOperator, UpdateResult};

pub(crate) struct UnsetOperator {
    fields: Vec<String>,
}

impl UnsetOperator {

    pub fn compile(doc: &Document) -> Result<UnsetOperator> {
        let fields = doc.keys().map(|k| k.to_string()).collect();
        Ok(UnsetOperator {
            fields
        })
    }

}

impl UpdateOperator for UnsetOperator {
    fn name(&self) -> &str {
        "unset"
    }

    fn update(&self, value: &mut Bson) -> Result<UpdateResult> {
        let doc = value.as_document_mut().unwrap();

        let mut updated = false;
        for field in &self.fields {
            doc.remove(field);
            updated = true;
        }

        Ok(UpdateResult {
            updated,
        })
    }
}
