use bson::Bson;
use bson::spec::ElementType;
use crate::{DbErr, DbResult};

pub fn stacked_key(keys: &[Bson]) -> DbResult<Vec<u8>> {
    let mut result = Vec::<u8>::new();

    for key in keys {
        match key {
            Bson::Double(dbl) => {
                result.push(ElementType::Double as u8);

                let val_be = dbl.to_be_bytes();
                for v in val_be {
                    result.push(v);
                }
            }
            Bson::String(str) => {
                result.push(ElementType::String as u8);

                for ch in str.chars() {
                    result.push(ch as u8);
                }

                result.push(0);
            }
            Bson::Boolean(bl) => {
                result.push(ElementType::Boolean as u8);

                result.push(*bl as u8);
            }
            Bson::Null => {
                result.push(ElementType::Null as u8);
            }
            Bson::Int32(i32) => {
                result.push(ElementType::Int32 as u8);

                let i32_be = i32.to_be_bytes();
                for v in i32_be {
                    result.push(v);
                }
            }
            Bson::Int64(i64) => {
                result.push(ElementType::Int64 as u8);

                let i64_be = i64.to_be_bytes();
                for v in i64_be {
                    result.push(v);
                }
            }
            Bson::Timestamp(ts) => {
                result.push(ElementType::Timestamp as u8);

                let u64 = ((ts.time as u64) << 32) | (ts.increment as u64);
                let u64_be = u64.to_be_bytes();

                for v in u64_be {
                    result.push(v);
                }
            }
            Bson::ObjectId(oid) => {
                result.push(ElementType::ObjectId as u8);

                let bytes = oid.bytes();
                for byte in bytes {
                    result.push(byte);
                }
            }
            Bson::DateTime(dt) => {
                result.push(ElementType::DateTime as u8);

                let t = dt.timestamp_millis();
                let t_be = t.to_be_bytes();

                for byte in t_be {
                    result.push(byte);
                }
            }
            Bson::Symbol(str) => {
                result.push(ElementType::Symbol as u8);

                for ch in str.chars() {
                    result.push(ch as u8);
                }

                result.push(0);
            }
            Bson::Decimal128(dcl) => {
                result.push(ElementType::Decimal128 as u8);
                let bytes = dcl.bytes();

                for byte in bytes {
                    result.push(byte);
                }
            }
            Bson::Undefined => {
                result.push(ElementType::Undefined as u8);
            }

            _ => {
                let val = format!("{:?}", key);
                return Err(DbErr::NotAValidKeyType(val))
            }
        }
    }

    Ok(result)
}
