use bson::{Bson, Document};
use crate::vm::update_operators::{UpdateOperator, UpdateResult};
use crate::{Error, Result};
use crate::errors::CannotApplyOperationForTypes;

pub(crate) struct IncOperator {
    doc: Document,
}

impl IncOperator {

    pub fn compile(doc: Document) -> Result<IncOperator> {
        <dyn UpdateOperator>::validate_key(&doc)?;
        Ok(IncOperator {
            doc
        })
    }

    fn inc_numeric(key: &str, a: &Bson, b: &Bson) -> Result<Bson> {
        let val = match (a, b) {
            (Bson::Int32(a), Bson::Int32(b)) => Bson::Int32(*a + *b),
            (Bson::Int32(a), Bson::Int64(b)) => Bson::Int64(*a as i64 + *b),
            (Bson::Int32(a), Bson::Double(b)) => Bson::Double(*a as f64 + *b),
            (Bson::Int64(a), Bson::Int64(b)) => Bson::Int64(*a + *b),
            (Bson::Int64(a), Bson::Int32(b)) => Bson::Int64(*a + *b as i64),
            (Bson::Int64(a), Bson::Double(b)) => Bson::Double(*a as f64 + *b),
            (Bson::Double(a), Bson::Double(b)) => Bson::Double(*a + *b),
            (Bson::Double(a), Bson::Int32(b)) => Bson::Double(*a + *b as f64),
            (Bson::Double(a), Bson::Int64(b)) => Bson::Double(*a + *b as f64),

            _ => {
                return Err(CannotApplyOperationForTypes {
                    op_name: "$inc".into(),
                    field_name: key.into(),
                    field_type: a.to_string(),
                    target_type: b.to_string(),
                }
                    .into());
            }
        };
        Ok(val)
    }

    fn inc_field(doc: &mut Document, key: &str, value: Bson) -> Result<()> {
        match doc.get(key) {
            Some(Bson::Null) => {
                return Err(Error::IncrementNullField);
            }

            Some(original_value) => {
                let result = IncOperator::inc_numeric(key, original_value, &value)?;
                doc.insert::<String, Bson>(key.into(), result);
            }

            None => {
                doc.insert::<String, Bson>(key.into(), value);
            }
        }
        Ok(())
    }

}

impl UpdateOperator for IncOperator {

    fn name(&self) -> &str {
        "inc"
    }

    fn update(&self, value: &mut Bson) -> Result<UpdateResult> {
        let doc = value.as_document_mut().unwrap();

        let mut updated = false;
        for (k, v) in self.doc.iter() {
            updated = true;
            IncOperator::inc_field(doc, k.as_str(), v.clone())?;
        }

        Ok(UpdateResult {
            updated,
        })
    }
}
