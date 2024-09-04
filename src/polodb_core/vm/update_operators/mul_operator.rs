use bson::{Bson, Document};
use crate::errors::CannotApplyOperationForTypes;
use crate::vm::update_operators::{UpdateOperator, UpdateResult};
use crate::Result;

pub(crate) struct MulOperator {
    doc: Document
}

impl MulOperator {

    pub fn compile(doc: Document) -> Result<MulOperator> {
        <dyn UpdateOperator>::validate_key(&doc)?;
        Ok(MulOperator {
            doc
        })
    }

    fn mul_numeric(key: &str, a: &Bson, b: &Bson) -> Result<Bson> {
        let val = match (a, b) {
            (Bson::Int32(a), Bson::Int32(b)) => Bson::Int32(*a * *b),
            (Bson::Int32(a), Bson::Int64(b)) => Bson::Int64(*a as i64 * *b),
            (Bson::Int32(a), Bson::Double(b)) => Bson::Double(*a as f64 * *b),
            (Bson::Int64(a), Bson::Int64(b)) => Bson::Int64(*a * *b),
            (Bson::Int64(a), Bson::Int32(b)) => Bson::Int64(*a * *b as i64),
            (Bson::Int64(a), Bson::Double(b)) => Bson::Double(*a as f64 * *b),
            (Bson::Double(a), Bson::Double(b)) => Bson::Double(*a * *b),
            (Bson::Double(a), Bson::Int32(b)) => Bson::Double(*a * *b as f64),
            (Bson::Double(a), Bson::Int64(b)) => Bson::Double(*a * *b as f64),

            _ => {
                return Err(CannotApplyOperationForTypes {
                    op_name: "$mul".into(),
                    field_name: key.into(),
                    field_type: a.to_string(),
                    target_type: b.to_string(),
                }
                    .into());
            }
        };
        Ok(val)
    }

    fn mul_field(doc: &mut Document, key: &str, value: Bson) -> Result<()> {
        match doc.get(key) {
            Some(original_value) => {
                let new_value = MulOperator::mul_numeric(key, original_value, &value)?;
                doc.insert::<String, Bson>(key.into(), new_value);
            }

            None => {
                doc.insert::<String, Bson>(key.into(), value);
            }
        }
        Ok(())
    }

}

impl UpdateOperator for MulOperator {

        fn name(&self) -> &str {
            "mul"
        }

        fn update(&self, value: &mut Bson) -> Result<UpdateResult> {
            let doc = value.as_document_mut().unwrap();

            let mut updated = false;
            for (k, v) in self.doc.iter() {
                MulOperator::mul_field(doc, k, v.clone())?;
                updated = true;
            }

            Ok(UpdateResult {
                updated,
            })
        }
}
