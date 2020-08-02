use super::value;
use super::linked_hash_map::LinkedHashMap;
use crate::bson::object_id::ObjectIdMaker;

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

}
