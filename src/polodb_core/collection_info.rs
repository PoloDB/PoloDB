/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::HashMap;
use bson::{Binary, DateTime};
use serde::{Deserialize, Serialize};
use indexmap::IndexMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexInfo {
    pub keys: IndexMap<String, i8>,
}

impl IndexInfo {

    pub fn single_index(name: String, order: i8) -> IndexInfo {
        let mut keys = IndexMap::new();
        keys.insert(name, order);
        IndexInfo {
            keys,
        }
    }

}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionSpecificationInfo {
    /// The collection's UUID - once established, this does not change and remains the same across
    /// replica set members and shards in a sharded cluster. If the data store is a view, this
    /// field is `None`.
    pub uuid: Option<Binary>,

    pub create_at: DateTime,

}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionSpecification {
    /// The name of the collection.
    #[serde(rename = "_id")]
    pub _id: String,

    /// Type of the data store.
    #[serde(rename = "type")]
    pub collection_type: CollectionType,

    /// Additional info pertaining to the collection.
    pub info: CollectionSpecificationInfo,

    /// name -> info
    pub indexes: HashMap<String, IndexInfo>,
}

impl CollectionSpecification {

    #[inline]
    pub fn name(&self) -> &str {
        self._id.as_str()
    }

}

/// Describes the type of data store returned when executing
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum CollectionType {
    /// Indicates that the data store is a view.
    View,

    /// Indicates that the data store is a collection.
    Collection,
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use bson::{Binary, DateTime};
    use bson::spec::BinarySubtype;
    use crate::collection_info::{CollectionSpecification, CollectionSpecificationInfo, CollectionType};

    #[test]
    fn test_serial() {
        let u = uuid::Uuid::new_v4();
        let spec = CollectionSpecification {
            _id: "test".to_string(),
            collection_type: CollectionType::Collection,
            info: CollectionSpecificationInfo {
                uuid: Some(Binary {
                    subtype: BinarySubtype::Uuid,
                    bytes: u.as_bytes().to_vec(),
                }),

                create_at: DateTime::now(),
            },
            indexes: HashMap::new(),
        };
        let doc = bson::to_document(&spec).unwrap();
        assert_eq!(doc.get("_id").unwrap().as_str().unwrap(), "test");

        let bytes = bson::to_vec(&doc).unwrap();
        assert_eq!(bytes.len(), 110);
    }

}
