use std::io::Write;
use bson::Bson;
use bson::spec::ElementType;
use byteorder::{BigEndian, WriteBytesExt};
use crate::{DbErr, DbResult};

pub fn stacked_key<'a, T: IntoIterator<Item = &'a Bson>>(keys: T) -> DbResult<Vec<u8>> {
    let mut result = Vec::<u8>::new();

    for key in keys {
        stacked_key_bytes(&mut result, key)?;
    }

    Ok(result)
}

pub fn stacked_key_bytes<W: Write>(writer: &mut W, key: &Bson) -> DbResult<()> {
    match key {
        Bson::Double(dbl) => {
            writer.write_u8(ElementType::Double as u8)?;
            writer.write_f64::<BigEndian>(*dbl)?;
        }
        Bson::String(str) => {
            writer.write_u8(ElementType::String as u8)?;

            writer.write_all(str.as_bytes())?;

            writer.write_u8(0)?;
        }
        Bson::Boolean(bl) => {
            writer.write_u8(ElementType::Boolean as u8)?;

            writer.write_u8(*bl as u8)?;
        }
        Bson::Null => {
            writer.write_u8(ElementType::Null as u8)?;
        }
        Bson::Int32(i32) => {
            writer.write_u8(ElementType::Int32 as u8)?;

            writer.write_i32::<BigEndian>(*i32)?;
        }
        Bson::Int64(i64) => {
            writer.write_u8(ElementType::Int64 as u8)?;

            writer.write_i64::<BigEndian>(*i64)?;
        }
        Bson::Timestamp(ts) => {
            writer.write_u8(ElementType::Timestamp as u8)?;

            let u64 = ((ts.time as u64) << 32) | (ts.increment as u64);

            writer.write_u64::<BigEndian>(u64)?;
        }
        Bson::ObjectId(oid) => {
            writer.write_u8(ElementType::ObjectId as u8)?;

            let bytes = oid.bytes();
            writer.write_all(&bytes)?;
        }
        Bson::DateTime(dt) => {
            writer.write_u8(ElementType::DateTime as u8)?;

            let t = dt.timestamp_millis();

            writer.write_i64::<BigEndian>(t)?;
        }
        Bson::Symbol(str) => {
            writer.write_u8(ElementType::Symbol as u8)?;

            writer.write_all(str.as_bytes())?;

            writer.write_u8(0)?;
        }
        Bson::Decimal128(dcl) => {
            writer.write_u8(ElementType::Decimal128 as u8)?;

            let bytes = dcl.bytes();

            writer.write_all(&bytes)?;
        }
        Bson::Undefined => {
            writer.write_u8(ElementType::Undefined as u8)?;
        }

        _ => {
            let val = format!("{:?}", key);
            return Err(DbErr::NotAValidKeyType(val))
        }
    }

    Ok(())
}
