use std::collections::HashMap;
use bson::{Binary, Bson, DateTime, Document};
use serde::{Deserialize, Serialize};
use crate::DbErr;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexInfo {
    key: Document,

    /// Internal
    #[serde(serialize_with = "crate::bson::serde_helpers::serialize_u32_as_i32")]
    pub root_pid: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionSpecificationInfo {
    /// The collection's UUID - once established, this does not change and remains the same across
    /// replica set members and shards in a sharded cluster. If the data store is a view, this
    /// field is `None`.
    pub uuid: Option<Binary>,

    pub create_at: DateTime,

    /// Internal
    #[serde(serialize_with = "crate::bson::serde_helpers::serialize_u32_as_i32")]
    pub root_pid: u32,
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
    use bson::{bson, Bson, DateTime};
    use crate::collection_info::{CollectionSpecification, CollectionSpecificationInfo, CollectionType};

    #[test]
    fn test_serial() {
        let spec = CollectionSpecification {
            _id: "test".to_string(),
            collection_type: CollectionType::Collection,
            info: CollectionSpecificationInfo {
                uuid: None,
                create_at: DateTime::now(),
                root_pid:1
            },
            indexes: HashMap::new(),
        };
        let doc = bson::to_document(&spec).unwrap();
        assert_eq!(doc.get("_id").unwrap().as_str().unwrap(), "test");
    }

}
