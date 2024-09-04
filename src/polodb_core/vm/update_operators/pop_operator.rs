use bson::{Bson, Document};
use indexmap::IndexMap;
use crate::{Result, Error};
use crate::errors::mk_invalid_query_field;
use crate::vm::update_operators::{UpdateOperator, UpdateResult};

pub(crate) struct PopOperator {
    pop_map: IndexMap<String, bool>,
}

impl PopOperator {

    pub fn compile(doc: Document, name: String, path: String) -> Result<PopOperator> {
        let mut pop_map = IndexMap::new();
        for (key, value) in doc.iter() {
            let num = match value {
                Bson::Int32(i) => *i as i64,
                Bson::Int64(i) => *i,
                _ => {
                    return Err(Error::InvalidField(mk_invalid_query_field(
                        name,
                        path
                    )))
                }
            };
            let val = match num {
                -1 => false,
                1 => true,
                _ => {
                    return Err(Error::InvalidField(mk_invalid_query_field(
                        name,
                        path
                    )))
                }
            };
            pop_map.insert(key.clone(), val);
        }
        Ok(PopOperator {
            pop_map,
        })
    }

}

impl UpdateOperator for PopOperator {

    fn name(&self) -> &str {
        "pop"
    }

    fn update(&self, value: &mut Bson) -> Result<UpdateResult> {
        let doc = value.as_document_mut().unwrap();

        let mut updated = false;
        for (k, is_first) in self.pop_map.iter() {
            let target = doc.get(k).unwrap_or(&Bson::Null);
            let result = match target.clone() {
                Bson::Array(mut arr) => {
                    if arr.is_empty() {
                        continue;
                    }
                    if *is_first {
                        arr.remove(0);
                    } else {
                        arr.pop();
                    }
                    Bson::Array(arr)
                }
                Bson::Null => {
                    Bson::Array(Vec::new())
                }
                _ => {
                    return Err(Error::InvalidField(mk_invalid_query_field(
                        self.name().to_string(),
                        k.to_string()
                    )))
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
