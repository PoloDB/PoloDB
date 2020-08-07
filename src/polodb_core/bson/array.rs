use std::io::Write;
use std::rc::Rc;
use super::value;
use crate::vli;
use crate::db::DbResult;
use crate::error::DbErr;
use crate::bson::{Document, ObjectId};

#[derive(Debug, Clone)]
pub struct Array {
    pub data: Vec<value::Value>,
}

impl Array {

    pub fn new() -> Array {
        let data = vec![];
        Array { data }
    }

    pub fn len(&self) -> u32 {
        self.data.len() as u32
    }

}

impl Array {

    pub fn to_bytes(&self) -> DbResult<Vec<u8>> {
        let mut result = vec![];

        vli::encode(&mut result, self.data.len() as i64)?;

        for item in &self.data {
            match item {
                value::Value::Null => {
                    result.push(0x0A);
                }

                value::Value::Double(num) => {
                    result.push(0x01);

                    result.extend_from_slice(&num.to_be_bytes());
                }

                value::Value::Boolean(bl) => {
                    result.push(0x08);

                    if *bl {
                        result.push(0x00);
                    } else {
                        result.push(0x01);
                    }
                }

                value::Value::Int(int_num) => {
                    result.push(0x16);  // not standard, use vli
                    vli::encode(&mut result, *int_num).expect("encode vli error");
                }

                value::Value::String(str) => {
                    result.push(0x02);

                    result.extend_from_slice(str.as_bytes());
                    result.push(0);
                }

                value::Value::ObjectId(oid) => {
                    result.push(0x07);

                    oid.serialize(&mut result)?;
                }

                value::Value::Array(arr) => {
                    result.push(0x17);

                    let buffer = arr.to_bytes()?;
                    vli::encode(&mut result, buffer.len() as i64)?;

                    result.extend(&buffer);
                }

                value::Value::Document(doc) => {
                    result.push(0x13);

                    let buffer = doc.to_bytes()?;
                    vli::encode(&mut result, buffer.len() as i64)?;

                    result.extend(&buffer);
                }

            }
        }

        result.push(0);

        Ok(result)
    }

    pub unsafe fn from_bytes(bytes: &[u8]) -> DbResult<Array> {
        let mut arr = Array::new();

        unsafe {

            let mut ptr = bytes.as_ptr();

            let (arr_len, to_ptr) = vli::decode_u64_raw(ptr)?;
            ptr = to_ptr;

            let mut counter: u64 = 0;
            while ptr.read() != 0 && counter < arr_len {
                let byte = ptr.read();
                ptr = ptr.add(1);

                match byte {
                    0x0A => {  // null
                        arr.data.push(value::Value::Null);
                    }

                    0x01 => {  // double
                        let mut buffer: [u8; 8] = [0; 8];
                        ptr.copy_to_nonoverlapping(buffer.as_mut_ptr(), 8);

                        let num = f64::from_be_bytes(buffer);
                        arr.data.push(value::Value::Double(num));

                        ptr = ptr.add(8);
                    }

                    0x08 => {  // boolean
                        let bl_value = ptr.read();
                        ptr = ptr.add(1);

                        arr.data.push(value::Value::Boolean(if bl_value != 0 {
                            true
                        } else {
                            false
                        }));
                    }

                    0x16 => {  // int
                        let (integer, to_ptr) = vli::decode_u64_raw(ptr)?;
                        ptr = to_ptr;

                        arr.data.push(value::Value::Int(integer as i64));
                    }

                    0x02 => {  // String
                        let (value, to_ptr) = Document::parse_key(ptr)?;
                        ptr = to_ptr;

                        arr.data.push(value::Value::String(value));
                    }

                    0x07 => {
                        let mut buffer: [u8; 12] = [0; 12];
                        ptr.copy_to_nonoverlapping(buffer.as_mut_ptr(), 12);

                        ptr = ptr.add(12);

                        let oid = ObjectId::deserialize(&buffer)?;

                        arr.data.push(value::Value::ObjectId(oid));
                    }

                    0x17 => {  // array
                        let (len, to_ptr) = vli::decode_u64_raw(ptr)?;
                        ptr = to_ptr;

                        let mut buffer = Vec::with_capacity(len as usize);
                        ptr.copy_to(buffer.as_mut_ptr(), len as usize);

                        ptr = ptr.add(len as usize);

                        let sub_arr = Array::from_bytes(&buffer)?;
                        arr.data.push(value::Value::Array(Rc::new(sub_arr)));
                    }

                    0x13 => {  // document
                        let (len, to_ptr) = vli::decode_u64_raw(ptr)?;
                        ptr = to_ptr;

                        let mut buffer = Vec::with_capacity(len as usize);
                        ptr.copy_to(buffer.as_mut_ptr(), len as usize);

                        ptr = ptr.add(len as usize);

                        let sub_doc = Document::from_bytes(&buffer)?;
                        arr.data.push(value::Value::Document(Rc::new(sub_doc)));
                    }

                    _ => return Err(DbErr::ParseError),
                }
                counter += 1;
            }

        }

        Ok(arr)
    }

}
