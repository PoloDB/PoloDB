use super::value;
use std::ptr::{null_mut};
use std::collections::{hash_map, HashMap};
use super::linked_hash_map::LinkedHashMap;
use crate::bson::value::Value::ObjectId;
use crate::bson::object_id::ObjectIdMaker;

// #[derive(Debug)]
// struct DocTuple {
//     key:        String,
//     value:      Box<value::Value>,
//     prev:       *mut DocTuple,
//     next:       *mut DocTuple,
// }

#[derive(Debug, Clone)]
pub struct Document {
    map: LinkedHashMap<String, value::Value>,
}

// fn mk_tuple(key: String, value: Box<value::Value>) -> DocTuple {
//     return DocTuple {
//         key,
//         value,
//         prev: null_mut(),
//         next: null_mut(),
//     }
// }

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
