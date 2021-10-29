
#[macro_use]
mod macros;

mod hex;
mod object_id;
mod document;
mod array;
mod value;
pub mod linked_hash_map;
pub mod error;
pub mod vli;
mod datetime;

pub use object_id::{ObjectId, ObjectIdMaker};
pub use document::Document;
pub use array::Array;
pub use datetime::UTCDateTime;
pub use value::*;
pub use error::BsonErr;

pub type BsonResult<T> = Result<T, error::BsonErr>;

#[cfg(test)]
mod tests {
    use crate::document::Document;
    use crate::object_id::ObjectIdMaker;

    #[test]
    fn document_basic() {
        let mut id_maker = ObjectIdMaker::new();
        let _doc = Document::new(&mut id_maker);
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn print_value_size() {
        let size = std::mem::size_of::<crate::Value>();
        assert_eq!(size, 16);
    }

}
