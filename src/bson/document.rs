use std::io::Write;
use super::value;
use super::linked_hash_map::LinkedHashMap;
use crate::vli;
use crate::db::DbResult;
use crate::bson::object_id::ObjectIdMaker;
use crate::serialization::DbSerializer;
use crate::error::DbErr;

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

    pub fn insert(&mut self, key: String, value: value::Value) -> Option<value::Value> {
        self.map.insert(key, value)
    }

    // little-endian
    fn to_bytes(&self) -> DbResult<Vec<u8>> {
        let mut result: Vec<u8> = vec![];

        // reserved 4 bytes for size
        result.extend_from_slice(&[ 0, 0, 0, 0 ]);

        for (key, value) in &self.map {
            match value {
                value::Value::Null => {
                    result.push(0x0A);

                    Document::key_to_bytes(&key, &mut result);
                }

                value::Value::Double(num) => {
                    result.push(0x01);

                    Document::key_to_bytes(&key, &mut result);

                    result.extend_from_slice(&num.to_be_bytes());
                }

                value::Value::Boolean(bl) => {
                    result.push(0x08);
                    Document::key_to_bytes(&key, &mut result);
                    if *bl {
                        result.push(0x00);
                    } else {
                        result.push(0x01);
                    }
                }

                value::Value::Int(int_num) => {
                    result.push(0x16);  // not standard, use vli
                    Document::key_to_bytes(&key, &mut result);
                    vli::encode(&mut result, *int_num).expect("encode vli error");
                }

                value::Value::String(str) => {
                    result.push(0x02);
                    Document::key_to_bytes(&key, &mut result);

                    let bytes = str.as_bytes();
                    let bytes_len = (bytes.len() + 1) as u32;

                    result.extend_from_slice(&bytes_len.to_le_bytes());
                    result.extend_from_slice(bytes);
                    result.push(0);
                }

                value::Value::ObjectId(oid) => {
                    result.push(0x07);
                    Document::key_to_bytes(&key, &mut result);

                    oid.serialize(&mut result)?;
                }

                value::Value::Array(arr) => {
                    result.push(0x17);  // not standard
                    Document::key_to_bytes(&key, &mut result);

                    arr.serialize(&mut result)?;
                }

                value::Value::Document(doc) => {
                    result.push(0x13);
                    Document::key_to_bytes(&key, &mut result);

                    doc.serialize(&mut result)?;
                }

            }

        }

        let actual_size: u32 = (result.len() as u32) - 4 + 1;
        result.push(0);
        result[0..4].copy_from_slice(&actual_size.to_le_bytes());

        Ok(result)
    }

    fn key_to_bytes(key: &str, data: &mut Vec<u8>) {
        data.extend_from_slice(key.as_bytes());
        data.push(0); // cstring end
    }

}

// spec
impl DbSerializer for Document {

    fn serialize(&self, writer: &mut dyn Write) -> DbResult<()> {
        let bytes = self.to_bytes()?;

        writer.write_all(bytes.as_ref())?;

        Ok(())
    }

}

#[cfg(test)]
mod tests {
    use crate::bson::value::Value;
    use crate::bson::document::Document;
    use crate::bson::object_id::ObjectIdMaker;
    use crate::serialization::DbSerializer;

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

        let mut bytes = vec![];
        doc.serialize(&mut bytes).expect("serialze error");

        let len = bytes.len();

        println!("len: {}", len)
    }

}
