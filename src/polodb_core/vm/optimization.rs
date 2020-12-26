use polodb_bson::{Document, Value};
use crate::DbResult;

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
