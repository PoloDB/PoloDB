use bson::{Bson, Document};
use crate::vm::update_operators::{UpdateOperator, UpdateResult};
use crate::Result;
use crate::errors::CannotApplyOperationForTypes;

pub(crate) struct PushOperator {
    doc: Document,
}

impl PushOperator {

    pub fn compile(doc: Document) -> Result<PushOperator> {
        <dyn UpdateOperator>::validate_key(&doc)?;
        Ok(PushOperator {
            doc
        })
    }

}

impl UpdateOperator for PushOperator {

    fn name(&self) -> &str {
        "push"
    }

    fn update(&self, value: &mut Bson) -> Result<UpdateResult> {
        let doc = value.as_document_mut().unwrap();

        let mut updated = false;
        for (k, v) in self.doc.iter() {
            let target = doc.get(k).unwrap_or(&Bson::Null);
            let result = match target.clone() {
                Bson::Array(mut arr) => {
                    arr.push(v.clone());
                    Bson::Array(arr)
                }
                Bson::Null => {
                    Bson::Array(vec![v.clone()])
                }
                _ => {
                    return Err(CannotApplyOperationForTypes {
                        op_name: "$push".into(),
                        field_name: k.into(),
                        field_type: target.to_string(),
                        target_type: v.to_string(),
                    }
                        .into());
                }
            };
            doc.insert(k.clone(), result);
            updated = true;
        }

        Ok(UpdateResult {
            updated,
        })
    }
}
