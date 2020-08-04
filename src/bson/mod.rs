pub mod hex;
pub mod object_id;
pub mod document;
pub mod array;
pub mod value;
mod linked_hash_map;

pub use object_id::ObjectId;
pub use document::Document;

#[cfg(test)]
mod tests {
    use crate::bson::document::Document;
    use crate::bson::object_id::ObjectIdMaker;

    #[test]
    fn document_basic() {
        let mut id_maker = ObjectIdMaker::new();
        let _doc = Document::new(&mut id_maker);
        assert_eq!(2 + 2, 4);
    }

}
