use bson::Bson;
use bson::ser::Error as BsonErr;
use bson::ser::Result as BsonResult;
use std::cmp::Ordering;

pub fn value_cmp(a: &Bson, b: &Bson) -> BsonResult<Ordering> {

    match (a, b) {
        (Bson::Null, Bson::Null) => Ok(Ordering::Equal),
        (Bson::Int64(i1), Bson::Int64(i2)) => Ok(i1.cmp(i2)),
        (Bson::Int32(i1), Bson::Int32(i2)) => Ok(i1.cmp(i2)),
        (Bson::Int64(i1), Bson::Int32(i2)) => {
            let i2_64 = *i2 as i64;
            Ok(i1.cmp(&i2_64))
        },
        (Bson::Int32(i1), Bson::Int64(i2)) => {
            let i1_64 = *i1 as i64;
            Ok(i1_64.cmp(i2))
        },
        (Bson::String(str1), Bson::String(str2)) => Ok(str1.cmp(str2)),
        (Bson::ObjectId(oid1), Bson::ObjectId(oid2)) => Ok(oid1.cmp(oid2)),
        _ => {
            // compare the numeric type
            let a_type = a.element_type() as u8;
            let b_type = b.element_type() as u8;
            if a_type != b_type {
                return Ok(a_type.cmp(&b_type));
            }

            Err(BsonErr::InvalidCString("Unsupported types".to_string()))
        },
    }
}

pub fn is_valid_key_type(value: &Bson) -> bool {
    matches!(value, Bson::String(_) |
                    Bson::Int64(_) |
                    Bson::ObjectId(_) |
                    Bson::Boolean(_))
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use bson::Bson;
    use crate::bson_utils::value_cmp;

    #[test]
    fn test_value_cmp() {
        assert_eq!(value_cmp(&Bson::Int32(2), &Bson::Int64(3)).unwrap(), Ordering::Less);
        assert_eq!(value_cmp(&Bson::Int32(2), &Bson::Int64(1)).unwrap(), Ordering::Greater);
        assert_eq!(value_cmp(&Bson::Int32(1), &Bson::Int64(1)).unwrap(), Ordering::Equal);
        assert_eq!(value_cmp(&Bson::Int64(2), &Bson::Int32(3)).unwrap(), Ordering::Less);
        assert_eq!(value_cmp(&Bson::Int64(2), &Bson::Int32(1)).unwrap(), Ordering::Greater);
        assert_eq!(value_cmp(&Bson::Int64(1), &Bson::Int32(1)).unwrap(), Ordering::Equal);
    }

}
