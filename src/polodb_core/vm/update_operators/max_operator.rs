use bson::{Bson, Document};
use crate::Result;
use crate::vm::op::{generic_cmp, DbOp};
use crate::vm::update_operators::{UpdateOperator, UpdateResult};

pub(crate) struct MaxOperator {
    doc: Document,
}

impl MaxOperator {

    pub fn compile(doc: Document) -> Result<MaxOperator> {
        <dyn UpdateOperator>::validate_key(&doc)?;
        Ok(MaxOperator {
            doc
        })
    }

}

impl UpdateOperator for MaxOperator {
    fn name(&self) -> &str {
        "max"
    }

    fn update(&self, value: &mut Bson) -> Result<UpdateResult> {
        let doc = value.as_document_mut().unwrap();

        let mut updated = false;
        for (k, v) in self.doc.iter() {
            let current_val = doc.get(k).unwrap_or(&Bson::Null);
            let cmp = generic_cmp(DbOp::Greater, v, current_val)?;
            if cmp {
                doc.insert(k.clone(), v.clone());
                updated = true;
            }
        }

        Ok(UpdateResult {
            updated,
        })
    }
}
