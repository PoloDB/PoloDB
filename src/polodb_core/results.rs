
use std::collections::{HashMap};
use crate::bson::Bson;
use serde::Serialize;

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
    pub inserted_ids: HashMap<usize, Bson>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateResult {
    /// The number of documents that were modified by the operation.
    #[serde(serialize_with = "crate::bson::serde_helpers::serialize_u64_as_i64")]
    pub modified_count: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteResult {
    /// The number of documents deleted by the operation.
    #[serde(serialize_with = "crate::bson::serde_helpers::serialize_u64_as_i64")]
    pub deleted_count: u64,
}
