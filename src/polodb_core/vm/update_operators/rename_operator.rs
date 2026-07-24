use crate::errors::FieldTypeUnexpectedStruct;
use crate::vm::update_operators::document_path::{rename_path, validate_update_path};
use crate::vm::update_operators::{UpdateOperator, UpdateResult};
use crate::Result;
use bson::{Bson, Document};

pub(crate) struct RenameOperator {
    doc: Document,
}

impl RenameOperator {
    pub fn compile(doc: Document) -> Result<RenameOperator> {
        <dyn UpdateOperator>::validate_key(&doc)?;
        for (key, value) in doc.iter() {
            let new_name = match value {
                Bson::String(new_name) => new_name.as_str(),
                t => {
                    return Err(FieldTypeUnexpectedStruct {
                        field_name: key.into(),
                        expected_ty: "String".into(),
                        actual_ty: t.to_string(),
                    }
                    .into());
                }
            };
            validate_update_path(new_name)?;
        }
        Ok(RenameOperator { doc })
    }
}

impl UpdateOperator for RenameOperator {
    fn name(&self) -> &str {
        "rename"
    }

    fn update(&self, value: &mut Bson) -> Result<UpdateResult> {
        let doc = value.as_document_mut().unwrap();

        let mut updated = false;
        for (source, destination) in self.doc.iter() {
            let Bson::String(destination) = destination else {
                unreachable!()
            };
            updated |= rename_path(doc, source, destination)?;
        }

        Ok(UpdateResult { updated })
    }
}
