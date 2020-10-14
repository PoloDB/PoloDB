use std::rc::Rc;
use std::ops;
use super::value::{Value, ty_int};
use crate::vli;
use crate::BsonResult;
use crate::error::{BsonErr, parse_error_reason};
use crate::document::Document;
use crate::object_id::ObjectId;

#[derive(Debug, Clone)]
pub struct Array(Vec<Value>);

impl Array {

    pub fn new() -> Array {
        let data = vec![];
        Array(data)
    }

    pub fn push(&mut self, elm: Value) {
        self.0.push(elm)
    }

    #[inline]
    pub fn len(&self) -> u32 {
        self.0.len() as u32
    }

}

impl Array {

    pub fn to_bytes(&self) -> BsonResult<Vec<u8>> {
        let mut result = vec![];

        vli::encode(&mut result, self.0.len() as i64)?;

        for item in &self.0 {
            match item {
                Value::Null => {
                    result.push(ty_int::NULL);
                }

                Value::Double(num) => {
                    result.push(ty_int::DOUBLE);

                    result.extend_from_slice(&num.to_be_bytes());
                }

                Value::Boolean(bl) => {
                    result.push(ty_int::BOOLEAN);

                    if *bl {
                        result.push(0x00);
                    } else {
                        result.push(0x01);
                    }
                }

                Value::Int(int_num) => {
                    result.push(ty_int::INT);  // not standard, use vli
                    vli::encode(&mut result, *int_num).expect("encode vli error");
                }

                Value::String(str) => {
                    result.push(ty_int::STRING);

                    result.extend_from_slice(str.as_bytes());
                    result.push(0);
                }

                Value::ObjectId(oid) => {
                    result.push(ty_int::OBJECT_ID);

                    oid.serialize(&mut result)?;
                }

                Value::Array(arr) => {
                    result.push(ty_int::ARRAY);

                    let buffer = arr.to_bytes()?;
                    vli::encode(&mut result, buffer.len() as i64)?;

                    result.extend(&buffer);
                }

                Value::Document(doc) => {
                    result.push(ty_int::DOCUMENT);

                    let buffer = doc.to_bytes()?;
                    vli::encode(&mut result, buffer.len() as i64)?;

                    result.extend(&buffer);
                }

                Value::Binary(bin) => {
                    result.push(ty_int::BINARY);

                    vli::encode(&mut result, bin.len() as i64)?;

                    result.extend(bin.as_ref());
                }

                Value::UTCDateTime(datetime) => {
                    result.push(ty_int::UTC_DATETIME);  // not standard, use vli
                    let ts = datetime.timestamp();
                    vli::encode(&mut result, ts as i64).expect("encode vli error");
                }

            }
        }

        result.push(0);

        Ok(result)
    }

    pub unsafe fn from_bytes(bytes: &[u8]) -> BsonResult<Array> {
        let mut arr = Array::new();

        let mut ptr = bytes.as_ptr();

        let (arr_len, to_ptr) = vli::decode_u64_raw(ptr)?;
        ptr = to_ptr;

        let mut counter: u64 = 0;
        while ptr.read() != 0 && counter < arr_len {
            let byte = ptr.read();
            ptr = ptr.add(1);

            match byte {
                ty_int::NULL => {
                    arr.0.push(Value::Null);
                }

                ty_int::DOUBLE => {
                    let mut buffer: [u8; 8] = [0; 8];
                    ptr.copy_to_nonoverlapping(buffer.as_mut_ptr(), 8);

                    let num = f64::from_be_bytes(buffer);
                    arr.0.push(Value::Double(num));

                    ptr = ptr.add(8);
                }

                ty_int::BOOLEAN => {
                    let bl_value = ptr.read();
                    ptr = ptr.add(1);

                    arr.0.push(Value::Boolean(if bl_value != 0 {
                        true
                    } else {
                        false
                    }));
                }

                ty_int::INT => {
                    let (integer, to_ptr) = vli::decode_u64_raw(ptr)?;
                    ptr = to_ptr;

                    arr.0.push(Value::Int(integer as i64));
                }

                ty_int::STRING => {
                    let (value, to_ptr) = Document::parse_key(ptr)?;
                    ptr = to_ptr;

                    arr.0.push(Value::String(Rc::new(value)));
                }

                ty_int::OBJECT_ID => {
                    let mut buffer: [u8; 12] = [0; 12];
                    ptr.copy_to_nonoverlapping(buffer.as_mut_ptr(), 12);

                    ptr = ptr.add(12);

                    let oid = ObjectId::deserialize(&buffer)?;

                    arr.0.push(Value::ObjectId(Rc::new(oid)));
                }

                ty_int::ARRAY => {
                    let (len, to_ptr) = vli::decode_u64_raw(ptr)?;
                    ptr = to_ptr;

                    let mut buffer = Vec::with_capacity(len as usize);
                    ptr.copy_to(buffer.as_mut_ptr(), len as usize);

                    ptr = ptr.add(len as usize);

                    let sub_arr = Array::from_bytes(&buffer)?;
                    arr.0.push(Value::Array(Rc::new(sub_arr)));
                }

                ty_int::DOCUMENT => {
                    let (len, to_ptr) = vli::decode_u64_raw(ptr)?;
                    ptr = to_ptr;

                    let mut buffer = Vec::with_capacity(len as usize);
                    ptr.copy_to(buffer.as_mut_ptr(), len as usize);

                    ptr = ptr.add(len as usize);

                    let sub_doc = Document::from_bytes(&buffer)?;
                    arr.0.push(Value::Document(Rc::new(sub_doc)));
                }

                ty_int::BINARY => {
                    let (len, to_ptr) = vli::decode_u64_raw(ptr)?;
                    ptr = to_ptr;

                    let mut buffer = Vec::with_capacity(len as usize);
                    ptr.copy_to(buffer.as_mut_ptr(), len as usize);

                    ptr = ptr.add(len as usize);

                    arr.0.push(Value::Binary(Rc::new(buffer)));
                }

                _ => return Err(BsonErr::ParseError(parse_error_reason::UNEXPECTED_DOCUMENT_FLAG.into())),
            }

            counter += 1;
        }

        Ok(arr)
    }

}

impl ops::Index<usize> for Array {
    type Output = Value;

    fn index(&self, index: usize) -> &Value {
        &self.0[index]
    }

}
