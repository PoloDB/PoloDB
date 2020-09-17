/*
 * Copyright (c) 2020 Vincent Chan
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation; either version 3, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
