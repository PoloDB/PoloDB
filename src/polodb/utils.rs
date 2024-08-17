use bson::{Bson, RawBsonRef, uuid};

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

pub(crate) fn uuid_from_bson(r: &Bson) -> Option<uuid::Uuid> {
    match r {
        Bson::Binary(bin) => {
            if bin.bytes.len() == 16 {
                let mut bytes: [u8; 16] = [0u8; 16];
                bytes.copy_from_slice(bin.bytes.as_slice());
                Some(uuid::Uuid::from_bytes(bytes))
            } else {
                None
            }
        },
        _ => None,
    }
}
