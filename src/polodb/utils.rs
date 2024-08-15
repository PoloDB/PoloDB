use bson::RawBsonRef;

pub(crate) fn truly_value_for_bson_ref(r: Option<RawBsonRef>, default: bool) -> bool {
    match r {
        Some(r) => {
            // bool or 1
            match r {
                RawBsonRef::Boolean(b) => b,
                RawBsonRef::Int32(i) => i == 1,
                RawBsonRef::Int64(i) => i == 1,
                _ => default,
            }
        },
        None => default,
    }
}
