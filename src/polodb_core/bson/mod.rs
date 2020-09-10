mod hex;
mod object_id;
mod document;
mod array;
mod value;
mod linked_hash_map;

pub use object_id::{ObjectId, ObjectIdMaker};
pub use document::Document;
pub use array::Array;
pub use value::*;

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
