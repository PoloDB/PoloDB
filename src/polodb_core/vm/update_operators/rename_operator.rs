use bson::{Bson, Document};
use crate::errors::FieldTypeUnexpectedStruct;
use crate::vm::update_operators::{UpdateOperator, UpdateResult};
use crate::Result;

pub(crate) struct RenameOperator {
    doc: Document
}

impl RenameOperator {

    pub fn compile(doc: Document) -> Result<RenameOperator> {
        for (key, value) in doc.iter() {
            let _new_name = match value {
                Bson::String(new_name) => new_name.as_str(),
                t => {
                    let name = format!("{}", t);
                    return Err(FieldTypeUnexpectedStruct {
                        field_name: key.into(),
                        expected_ty: "String".into(),
                        actual_ty: name,
                    }
                        .into());
                }
            };
        }
        Ok(RenameOperator {
            doc
        })
    }

    fn rename_field(doc: &mut Document, key: &str, new_key: &str) -> Result<()> {
        match doc.remove(key) {
            Some(value) => {
                doc.insert(new_key, value);
            }

            None => {
                doc.insert(new_key, Bson::Null);
            }
        }
        Ok(())
    }

}

impl UpdateOperator for RenameOperator {

    fn name(&self) -> &str {
        "rename"
    }

    fn update(&self, value: &mut Bson) -> Result<UpdateResult> {
        let doc = value.as_document_mut().unwrap();

        let mut updated = false;
        for (k, v) in self.doc.iter() {
            let new_name = match v {
                Bson::String(new_name) => new_name.as_str(),
                _ => unreachable!()
            };
            RenameOperator::rename_field(doc, k, new_name)?;
            updated = true;
        }

        Ok(UpdateResult {
            updated,
        })
    }

}
