use bson::Bson;
use bson::ser::Error as BsonErr;
use bson::ser::Result as BsonResult;
use std::cmp::Ordering;

pub fn value_cmp(a: &Bson, b: &Bson) -> BsonResult<Ordering> {
    match (a, b) {
        (Bson::Null, Bson::Null) => Ok(Ordering::Equal),
        (Bson::Int64(i1), Bson::Int64(i2)) => Ok(i1.cmp(i2)),
        (Bson::String(str1), Bson::String(str2)) => Ok(str1.cmp(str2)),
        (Bson::ObjectId(oid1), Bson::ObjectId(oid2)) => Ok(oid1.cmp(oid2)),
        _ => Err(BsonErr::InvalidCString("Unsupported types".to_string())),
    }
}
