use polodb_bson::{Document, Value, Array};
use crate::error::mk_field_name_type_unexpected;
use crate::{DbResult, DbErr};

macro_rules! try_unwrap_document {
    ($op_name:tt, $doc:expr) => {
        match $doc {
            Value::Document(doc) => doc,
            t => {
                let err = mk_field_name_type_unexpected($op_name, "Document".into(), t.ty_name());
                return Err(err);
            },
        }
    };
}

fn inverse_key(key: &str) -> &str {
    if let Some('$') = key.chars().next() {
        return match key {
            "$and" => "$or",
            "$or"  => "$and",
            "$eq"  => "$ne",
            "$gt"  => "$lte",
            "$gte" => "$le",
            "$in"  => "$nin",
            "$lt"  => "$gte",
            "$lte" => "$gt",
            "$ne"  => "$eq",
            "$nin" => "$in",
            _ => panic!("unknown op: {}", key),
        }
    }

    key
}

pub(super) fn inverse_doc(doc: &Document) -> DbResult<Document> {
    let mut result = Document::new_without_id();

    for (key, value) in doc.iter() {
        let inverse_key = inverse_key(key);
        let inverse_value: Value = match value {
            Value::Document(doc) => {
                let inverse_doc = inverse_doc(doc)?;
                inverse_doc.into()
            },

            _ => value.clone(),
        };

        result.insert(inverse_key.into(), inverse_value);
    }

    Ok(result)
}

fn merge_logic_and_compare(key: &str, value1: &Value, value2: &Value) -> DbResult<Value> {
    match key {
        "$and" => unimplemented!(),
        "$or"  => unimplemented!(),
        "$eq"  => unimplemented!(),
        "$gt"  => unimplemented!(),
        "$gte" => unimplemented!(),
        "$in"  => unimplemented!(),
        "$lt"  => unimplemented!(),
        "$lte" => unimplemented!(),
        "$ne"  => unimplemented!(),
        "$nin" => unimplemented!(),
        _ => Err(DbErr::NotAValidField(key.into())),
    }
}

fn merge_logic_inner_operation(mut exist_doc: Document, query_doc: &Document) -> DbResult<Document> {
    // key is operation such as $gt/$lte
    for (key, value) in query_doc.iter() {
        match exist_doc.get(key) {
            Some(exist_value) => {

            }

            None => {
                exist_doc.insert(key.into(), value.clone());
            }
        }
    }

    Ok(exist_doc)
}

///
/// [
///     {
///         age: {
///             $gt: 3,
///         }
///     },
///     {
///         age: {
///             $lte: 18,
///         }
///     },
/// ]
/// ==>
/// {
///     age: {
///         $gt: 3,
///         $lte: 18,
///     }
/// }
///
pub(super) fn merge_logic_and_array(arr: Array) -> DbResult<Document> {
    let mut result = Document::new_without_id();

    for doc_value in arr.iter() {
        let query_doc = try_unwrap_document!("$and", doc_value);
        for (key, value) in query_doc.iter() {
            match result.get(key) {
                Some(exist_value) => {  // same field
                    let exist_doc = exist_value.unwrap_document();
                    let new_doc = merge_logic_inner_operation(
                        query_doc.as_ref().clone(), exist_doc)?;
                    result.insert(key.into(), new_doc.into());
                }

                None => {
                    result.insert(key.into(), value.clone());
                }

            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use polodb_bson::mk_document;
    use crate::vm::optimization::inverse_doc;

    #[test]
    fn test_inverse() {
        let test_doc = mk_document! {
            "$gt": mk_document! {
                "age": 18
            },
        };

        let tmp = inverse_doc(&test_doc).unwrap();
        assert!(tmp.get("$gt").is_none());
        assert!(tmp.get("$lte").is_some());
    }

}
