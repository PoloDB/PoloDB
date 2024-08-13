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

use std::collections::{HashMap};
use crate::bson::Bson;
use serde::{Serialize, Serializer};
use serde::ser::SerializeMap;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InsertOneResult {
    /// The `_id` field of the document inserted.
    pub inserted_id: Bson,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InsertManyResult {
    /// The `_id` field of the documents inserted.
    #[serde(serialize_with = "map_serialize")]
    pub inserted_ids: HashMap<usize, Bson>,
}

fn map_serialize<S>(data: &HashMap<usize, Bson>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
{
    let mut map = serializer.serialize_map(Some(1))?;

    for (size, value) in data {
        let size_str = size.to_string();
        map.serialize_entry(&size_str, value)?;
    }

    map.end()
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateResult {
    /// The number of documents that were modified by the operation.
    #[serde(serialize_with = "crate::bson::serde_helpers::serialize_u64_as_i64")]
    pub modified_count: u64,
}

impl Default for UpdateResult {
    fn default() -> Self {
        UpdateResult {
            modified_count: 0,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteResult {
    /// The number of documents deleted by the operation.
    #[serde(serialize_with = "crate::bson::serde_helpers::serialize_u64_as_i64")]
    pub deleted_count: u64,
}

impl Default for DeleteResult {
    fn default() -> Self {
        DeleteResult {
            deleted_count: 0,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CountDocumentsResult {
    pub count: u64,
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use std::collections::HashMap;
    use crate::bson::Bson;
    use crate::results::InsertManyResult;

    #[test]
    fn test_serde_insert_result() {
        let mut inserted_ids: HashMap<usize, Bson> = HashMap::new() ;
        inserted_ids.insert(0, doc! {}.into());

        let result = InsertManyResult { inserted_ids };
        let _bson_doc = bson::to_document(&result).unwrap();
        let bson_str = format!("{:?}", _bson_doc);
        assert_eq!(r#"Document({"insertedIds": Document({"0": Document({})})})"#, bson_str);
    }

}
