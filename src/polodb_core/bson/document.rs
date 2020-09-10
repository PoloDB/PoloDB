use std::rc::Rc;
use std::fmt;
use super::value;
use super::linked_hash_map::{LinkedHashMap, Iter};
use crate::vm::vli;
use crate::db::DbResult;
use crate::bson::object_id::{ ObjectIdMaker, ObjectId };
use crate::error::{DbErr, parse_error_reason};
use crate::bson::array::Array;

#[derive(Debug, Clone)]
pub struct Document {
    map: LinkedHashMap<String, value::Value>,
}

impl Document {

    pub fn new(id_maker: &mut ObjectIdMaker) -> Document {
        let id = id_maker.mk_object_id();
        let mut result = Document {
            map: LinkedHashMap::new(),
        };
        result.map.insert("_id".to_string(), value::Value::ObjectId(id));
        result
    }

    pub fn new_without_id() -> Document {
        Document {
            map: LinkedHashMap::new(),
        }
    }

    #[inline]
    pub fn insert(&mut self, key: String, value: value::Value) -> Option<value::Value> {
        self.map.insert(key, value)
    }

    #[inline]
    pub fn get(&self, key: &str) -> Option<&value::Value> {
        self.map.get(key)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn pkey_id(&self) -> Option<value::Value> {
        self.map.get("_id".into()).map(|id| { id.clone() })
    }

    pub fn from_bytes(bytes: &[u8]) -> DbResult<Document> {
        let mut doc = Document::new_without_id();

        unsafe {
            let mut ptr = bytes.as_ptr();
            while ptr.read() != 0 {
                let byte = ptr.read();
                ptr = ptr.add(1);

                match byte {
                    0x0A => {  // null
                        let (key, to_ptr) = Document::parse_key(ptr)?;
                        ptr = to_ptr;

                        doc.map.insert(key, value::Value::Null);
                    }

                    0x01 => {  // double
                        let (key, to_ptr) = Document::parse_key(ptr)?;
                        ptr = to_ptr;

                        let mut buffer: [u8; 8] = [0; 8];
                        ptr.copy_to_nonoverlapping(buffer.as_mut_ptr(), 8);

                        let num = f64::from_be_bytes(buffer);
                        doc.map.insert(key, value::Value::Double(num));

                        ptr = ptr.add(8);
                    }

                    0x08 => {  // boolean
                        let (key, to_ptr) = Document::parse_key(ptr)?;
                        ptr = to_ptr;

                        let bl_value = ptr.read();
                        ptr = ptr.add(1);

                        doc.map.insert(key, value::Value::Boolean(if bl_value != 0 {
                            true
                        } else {
                            false
                        }));
                    }

                    0x16 => {  // int
                        let (key, to_ptr) = Document::parse_key(ptr)?;
                        ptr = to_ptr;

                        let (integer, to_ptr) = vli::decode_u64_raw(ptr)?;
                        ptr = to_ptr;

                        doc.map.insert(key, value::Value::Int(integer as i64));
                    }

                    0x02 => {  // String
                        let (key, to_ptr) = Document::parse_key(ptr)?;
                        ptr = to_ptr;

                        let (value, to_ptr) = Document::parse_key(ptr)?;
                        ptr = to_ptr;

                        doc.map.insert(key, value::Value::String(value));
                    }

                    0x07 => {  // ObjectId
                        let (key, to_ptr) = Document::parse_key(ptr)?;
                        ptr = to_ptr;

                        let mut buffer: [u8; 12] = [0; 12];
                        ptr.copy_to_nonoverlapping(buffer.as_mut_ptr(), 12);

                        ptr = ptr.add(12);

                        let oid = ObjectId::deserialize(&buffer)?;

                        doc.map.insert(key, value::Value::ObjectId(oid));
                    }

                    0x17 => {  // array
                        let (key, to_ptr) = Document::parse_key(ptr)?;
                        ptr = to_ptr;

                        let (len, to_ptr) = vli::decode_u64_raw(ptr)?;
                        ptr = to_ptr;

                        let mut buffer = Vec::with_capacity(len as usize);
                        ptr.copy_to(buffer.as_mut_ptr(), len as usize);

                        ptr = ptr.add(len as usize);

                        let sub_arr = Array::from_bytes(&buffer)?;
                        doc.map.insert(key, value::Value::Array(Rc::new(sub_arr)));
                    }

                    0x13 => {  // document
                        let (key, to_ptr) = Document::parse_key(ptr)?;
                        ptr = to_ptr;

                        let (len, to_ptr) = vli::decode_u64_raw(ptr)?;
                        ptr = to_ptr;

                        let mut buffer = Vec::with_capacity(len as usize);
                        ptr.copy_to(buffer.as_mut_ptr(), len as usize);

                        ptr = ptr.add(len as usize);

                        let sub_doc = Document::from_bytes(&buffer)?;

                        doc.map.insert(key, value::Value::Document(Rc::new(sub_doc)));
                    }

                    _ => return Err(DbErr::ParseError(parse_error_reason::UNEXPECTED_DOCUMENT_FLAG.into())),
                }

            }
        }

        Ok(doc)
    }

    pub unsafe fn parse_key(ptr: *const u8) -> DbResult<(String, *const u8)> {
        let mut ptr = ptr;
        let mut buffer = Vec::with_capacity(128);
        while ptr.read() != 0 {
            buffer.push(ptr.read());
            ptr = ptr.add(1);
        }

        Ok((String::from_utf8_unchecked(buffer), ptr.add(1)))
    }

    fn value_to_bytes(key: &str, value: &value::Value, buffer: &mut Vec<u8>) -> DbResult<()> {
        match value {
            value::Value::Null => {
                buffer.push(0x0A);

                Document::key_to_bytes(&key, buffer);
            }

            value::Value::Double(num) => {
                buffer.push(0x01);

                Document::key_to_bytes(&key, buffer);

                buffer.extend_from_slice(&num.to_be_bytes());
            }

            value::Value::Boolean(bl) => {
                buffer.push(0x08);
                Document::key_to_bytes(&key, buffer);
                if *bl {
                    buffer.push(0x00);
                } else {
                    buffer.push(0x01);
                }
            }

            value::Value::Int(int_num) => {
                buffer.push(0x16);  // not standard, use vli
                Document::key_to_bytes(&key, buffer);
                vli::encode(buffer, *int_num).expect("encode vli error");
            }

            value::Value::String(str) => {
                buffer.push(0x02);
                Document::key_to_bytes(&key, buffer);

                Document::key_to_bytes(&str, buffer);
            }

            value::Value::ObjectId(oid) => {
                buffer.push(0x07);
                Document::key_to_bytes(&key, buffer);

                oid.serialize(buffer)?;
            }

            value::Value::Array(arr) => {
                buffer.push(0x17);  // not standard
                Document::key_to_bytes(&key, buffer);

                let tmp = arr.to_bytes()?;
                vli::encode( buffer, tmp.len() as i64)?;

                buffer.extend(&tmp);
            }

            value::Value::Document(doc) => {
                buffer.push(0x13);
                Document::key_to_bytes(&key, buffer);

                let tmp = doc.to_bytes()?;
                vli::encode(buffer, tmp.len() as i64)?;

                buffer.extend(&tmp);
            }
        }

        Ok(())
    }

    pub fn to_bytes(&self) -> DbResult<Vec<u8>> {
        let mut result: Vec<u8> = vec![];

        // insert id first
        let id = self.map.get("_id").ok_or(DbErr::DataHasNoPrimaryKey)?;
        Document::value_to_bytes("_id", id, &mut result)?;

        for (key, value) in &self.map {
            if key == "_id" {
                continue;
            }

            Document::value_to_bytes(key, value, &mut result)?;
        }

        result.push(0);

        Ok(result)
    }

    #[inline]
    pub fn iter(&self) -> Iter<String, value::Value> {
        self.map.iter()
    }

    fn key_to_bytes(key: &str, data: &mut Vec<u8>) {
        data.extend_from_slice(key.as_bytes());
        data.push(0); // cstring end
    }

}

#[cfg(test)]
mod tests {
    use crate::bson::value::Value;
    use crate::bson::document::Document;
    use crate::bson::object_id::ObjectIdMaker;

    #[test]
    fn test_serialize() {
        let mut id_maker = ObjectIdMaker::new();
        let mut doc = Document::new(&mut id_maker);

        doc.map.insert("avater_utl".into(), Value::String("https://doc.rust-lang.org/std/iter/trait.Iterator.html".into()));
        doc.map.insert("name".into(), Value::String("嘻嘻哈哈".into()));
        doc.map.insert("groupd_id".into(), Value::String("70xxx80057ba0bba964fxxx1ca3d7252fe075a8b".into()));
        doc.map.insert("user_id".into(), Value::String("6500xxx139040719xxx".into()));
        doc.map.insert("time".into(), Value::Int(6662496067319235000));
        doc.map.insert("can_do_a".into(), Value::Boolean(true));
        doc.map.insert("can_do_b".into(), Value::Boolean(false));
        doc.map.insert("can_do_c".into(), Value::Boolean(false));

        let bytes = doc.to_bytes().expect("serial error");

        let parsed_doc = Document::from_bytes(&bytes).expect("deserialize error");

        assert_eq!(parsed_doc.len(), doc.len());
    }

}

impl fmt::Display for Document {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{ ")?;

        let mut index = 0;
        for (key, value) in &self.map {
            write!(f, "{}: {}", key, value)?;

            if index < self.map.len() - 1 {
                write!(f, ", ")?;
            }
            index += 1;
        }

        write!(f, " }}")
    }

}
