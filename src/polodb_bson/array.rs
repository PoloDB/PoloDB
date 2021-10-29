use std::ops;
use std::rc::Rc;
use super::value::{Value, ty_int};
use crate::{vli, UTCDateTime};
use crate::BsonResult;
use crate::error::{BsonErr, parse_error_reason};
use crate::document::Document;
use crate::object_id::ObjectId;
use std::vec::Drain;
use std::ops::RangeBounds;
use std::io::Read;

#[derive(Debug, Clone)]
pub struct Array(Vec<Value>);

pub struct Iter<'a> {
    arr: &'a Array,
    index: u32,
}

impl<'a> std::iter::Iterator for Iter<'a> {
    type Item = &'a Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.arr.len() {
            return None;
        }
        let result: &'a Value = &self.arr[self.index as usize];
        self.index += 1;
        Some(result)
    }
}

impl Array {

    pub fn new() -> Array {
        let data = vec![];
        Array(data)
    }

    pub fn new_with_size(size: usize) -> Array {
        let mut data = Vec::new();
        data.resize(size, Value::Null);
        Array(data)
    }

    pub fn iter(&self) -> Iter {
        Iter {
            arr: self,
            index: 0,
        }
    }

    pub fn push(&mut self, elm: Value) {
        self.0.push(elm)
    }

    pub fn pop(&mut self) -> Option<Value> {
        self.0.pop()
    }

    pub fn drain<R>(&mut self, range: R) -> Drain<'_, Value>
    where
        R: RangeBounds<usize>,
    {
        self.0.drain(range)
    }

    #[inline]
    pub fn len(&self) -> u32 {
        self.0.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
                    vli::encode(&mut result, ts as i64)?;
                }

            }
        }

        result.push(0);

        Ok(result)
    }

    pub fn from_bytes(bytes: &[u8]) -> BsonResult<Array> {
        let mut arr = Array::new();

        let mut ptr: usize = 0;

        let (arr_len, offset) = vli::decode_u64(&bytes[ptr..])?;
        ptr += offset;

        let mut counter: u64 = 0;
        while bytes[ptr] != 0 && counter < arr_len {
            let byte = bytes[ptr];
            ptr += 1;

            match byte {
                ty_int::NULL => {
                    arr.0.push(Value::Null);
                }

                ty_int::DOUBLE => {
                    let mut buffer: [u8; 8] = [0; 8];
                    buffer.copy_from_slice(&bytes[ptr..(ptr+8)]);

                    let num = f64::from_be_bytes(buffer);
                    arr.0.push(Value::Double(num));

                    ptr += 8;
                }

                ty_int::BOOLEAN => {
                    let bl_value = bytes[ptr];
                    ptr += 1;

                    arr.0.push(Value::Boolean(bl_value != 0));
                }

                ty_int::INT => {
                    let (integer, offset) = vli::decode(&bytes[ptr..])?;
                    ptr += offset;

                    arr.0.push(Value::Int(integer));
                }

                ty_int::STRING => {
                    let (value, to_ptr) = Document::parse_key(bytes, ptr)?;
                    ptr = to_ptr;

                    arr.0.push(value.into());
                }

                ty_int::OBJECT_ID => {
                    let mut buffer: [u8; 12] = [0; 12];
                    buffer.copy_from_slice(&bytes[ptr..(ptr+12)]);

                    ptr += 12;

                    let oid = ObjectId::deserialize(&buffer)?;

                    arr.0.push(oid.into());
                }

                ty_int::ARRAY => {
                    let (len, offset) = vli::decode_u64(&bytes[ptr..])?;
                    ptr += offset;

                    let mut buffer = Vec::with_capacity(len as usize);
                    buffer.extend_from_slice(&bytes[ptr..(ptr+len as usize)]);

                    ptr += len as usize;

                    let sub_arr = Array::from_bytes(&buffer)?;
                    arr.0.push(sub_arr.into());
                }

                ty_int::DOCUMENT => {
                    let (len, offset) = vli::decode_u64(&bytes[ptr..])?;
                    ptr += offset;

                    let mut buffer = Vec::with_capacity(len as usize);
                    buffer.extend_from_slice(&bytes[ptr..(ptr+len as usize)]);

                    ptr += len as usize;

                    let sub_doc = Document::from_bytes(&buffer)?;
                    arr.0.push(sub_doc.into());
                }

                ty_int::BINARY => {
                    let (len, offset) = vli::decode_u64(&bytes[ptr..])?;
                    ptr += offset;

                    let mut buffer = Vec::with_capacity(len as usize);
                    buffer.extend_from_slice(&bytes[ptr..(ptr+len as usize)]);

                    ptr += len as usize;

                    arr.0.push(buffer.into());
                }

                ty_int::UTC_DATETIME => {
                    let (integer, offset) = vli::decode_u64(&bytes[ptr..])?;
                    ptr += offset;

                    arr.0.push(Value::UTCDateTime(Rc::new(UTCDateTime::new(integer))));
                }

                _ => return Err(BsonErr::ParseError(parse_error_reason::UNEXPECTED_DOCUMENT_FLAG.into())),
            }

            counter += 1;
        }

        Ok(arr)
    }

    pub fn to_msgpack(&self, buf: &mut Vec<u8>) -> BsonResult<()> {
        rmp::encode::write_array_len(buf, self.len())?;
        for value in self.iter() {
            value.to_msgpack(buf)?;
        }
        Ok(())
    }

    pub fn from_msgpack<R: Read>(bytes: &mut R) -> BsonResult<Array> {
        let arr_len = rmp::decode::read_array_len(bytes)? as usize;
        Array::from_msgpack_with_len(bytes, arr_len)
    }

    pub fn from_msgpack_with_len<R: Read>(bytes: &mut R, len: usize) -> BsonResult<Array> {
        let mut buf: Vec<Value> = Vec::with_capacity(len);

        for _ in 0..len {
            let value = Value::from_msgpack(bytes)?;
            buf.push(value);
        }

        Ok(Array(buf))
    }

}

impl ops::Index<usize> for Array {
    type Output = Value;

    fn index(&self, index: usize) -> &Value {
        &self.0[index]
    }

}

impl ops::IndexMut<usize> for Array {

    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
    }

}

impl Default for Array {

    fn default() -> Self {
        Self::new()
    }

}
