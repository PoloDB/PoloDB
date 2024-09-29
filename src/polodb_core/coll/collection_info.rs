// Copyright 2024 Vincent Chan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bson::{Binary, DateTime};
use bson::spec::BinarySubtype;
use serde::{Deserialize, Serialize};
use indexmap::IndexMap;
use uuid::Uuid;
use crate::IndexOptions;
use crate::utils::bson::bson_datetime_now;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexInfo {
    /// The key of the map is preserving the original format
    /// For example, `author.age` is "author.age"
    pub keys: IndexMap<String, i8>,

    pub options: Option<IndexOptions>,
}

impl IndexInfo {

    pub fn single_index(name: String, order: i8, options: Option<IndexOptions>) -> IndexInfo {
        let mut keys = IndexMap::new();
        keys.insert(name, order);
        IndexInfo {
            keys,
            options,
        }
    }

    #[inline]
    pub fn is_unique(&self) -> bool {
        self.options
            .as_ref()
            .and_then(|options| options.unique)
            .unwrap_or(false)
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
    /// The name is converted to the underline format.
    /// For examples, `author.age` is converted to `author_age`
    pub indexes: IndexMap<String, IndexInfo>,
}

impl CollectionSpecification {

    #[inline]
    pub fn name(&self) -> &str {
        self._id.as_str()
    }

    #[inline]
    pub(crate) fn new(id: String, uuid: Uuid) -> CollectionSpecification {
        CollectionSpecification {
            _id: id,

            collection_type: CollectionType::Collection,
            info: CollectionSpecificationInfo {
                uuid: Some(Binary {
                    subtype: BinarySubtype::Uuid,
                    bytes: uuid.as_bytes().to_vec(),
                }),
                create_at: bson_datetime_now(),
            },

            indexes: IndexMap::new(),
        }
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
    use crate::coll::collection_info::{
        CollectionSpecification,
    };

    #[test]
    fn test_serial() {
        let u = uuid::Uuid::new_v4();
        let spec = CollectionSpecification::new("test".to_string(), u);
        let doc = bson::to_document(&spec).unwrap();
        assert_eq!(doc.get("_id").unwrap().as_str().unwrap(), "test");

        let bytes = bson::to_vec(&doc).unwrap();
        assert_eq!(bytes.len(), 110);
    }

}
