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

use bson::{doc, Bson, Document};
use polodb_core::{Collection, CollectionT, IndexModel};

mod common;

use common::prepare_db;

fn matching_ids(collection: &Collection<Document>, filter: Document) -> Vec<String> {
    let mut ids = collection
        .find(filter)
        .run()
        .unwrap()
        .map(|result| result.unwrap().get_str("_id").unwrap().to_owned())
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn assert_matching_ids(collection: &Collection<Document>, filter: Document, expected: &[&str]) {
    let mut expected = expected
        .iter()
        .map(|id| (*id).to_owned())
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(matching_ids(collection, filter), expected);
}

#[test]
fn test_equality_matches_array_elements_and_exact_arrays() {
    let db = prepare_db("test-query-equality-array-semantics").unwrap();
    let collection = db.collection::<Document>("items");

    collection
        .insert_many(vec![
            doc! { "_id": "scalar", "value": 2 },
            doc! { "_id": "array", "value": [1, 2] },
            doc! { "_id": "reordered", "value": [2, 1] },
            doc! { "_id": "nested", "value": [[1, 2], 3] },
            doc! { "_id": "empty", "value": [] },
            doc! { "_id": "nested_empty", "value": [[]] },
        ])
        .unwrap();

    assert_matching_ids(
        &collection,
        doc! { "value": Bson::Int64(2) },
        &["array", "reordered", "scalar"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$eq": Bson::Double(2.0) } },
        &["array", "reordered", "scalar"],
    );

    let exact_array = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]);
    assert_matching_ids(
        &collection,
        doc! { "value": exact_array.clone() },
        &["array", "nested"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$eq": exact_array } },
        &["array", "nested"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": Bson::Array(Vec::new()) },
        &["empty", "nested_empty"],
    );
}

#[test]
fn test_null_equality_matches_missing_null_and_array_elements() {
    let db = prepare_db("test-query-equality-null-semantics").unwrap();
    let collection = db.collection::<Document>("items");

    collection
        .insert_many(vec![
            doc! { "_id": "missing" },
            doc! { "_id": "null", "value": Bson::Null },
            doc! { "_id": "array_null", "value": [Bson::Null, Bson::String("x".into())] },
            doc! { "_id": "empty", "value": [] },
            doc! { "_id": "scalar", "value": "x" },
        ])
        .unwrap();

    let expected = &["array_null", "missing", "null"];
    assert_matching_ids(&collection, doc! { "value": Bson::Null }, expected);
    assert_matching_ids(
        &collection,
        doc! { "value": { "$eq": Bson::Null } },
        expected,
    );
}

#[test]
fn test_ne_and_not_include_missing_fields_except_for_null() {
    let db = prepare_db("test-query-ne-null-and-missing-semantics").unwrap();
    let collection = db.collection::<Document>("items");

    collection
        .insert_many(vec![
            doc! { "_id": "rock", "value": ["rock", "live"] },
            doc! { "_id": "jazz", "value": ["jazz"] },
            doc! { "_id": "missing" },
            doc! { "_id": "null", "value": Bson::Null },
            doc! { "_id": "array_null", "value": [Bson::Null, Bson::String("jazz".into())] },
            doc! { "_id": "empty", "value": [] },
        ])
        .unwrap();

    let not_rock = &["array_null", "empty", "jazz", "missing", "null"];
    assert_matching_ids(&collection, doc! { "value": { "$ne": "rock" } }, not_rock);
    assert_matching_ids(
        &collection,
        doc! { "value": { "$not": { "$eq": "rock" } } },
        not_rock,
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$ne": Bson::Null } },
        &["empty", "jazz", "rock"],
    );
}

#[test]
fn test_null_undefined_and_array_equality_bypass_index_lookup() {
    let db = prepare_db("test-query-equality-index-bypass").unwrap();
    let metrics = db.metrics();
    let collection = db.collection::<Document>("items");

    collection
        .insert_many(vec![
            doc! { "_id": "missing" },
            doc! { "_id": "null", "value": Bson::Null },
            doc! { "_id": "array", "value": [1, 2] },
            doc! { "_id": "undefined", "value": Bson::Undefined },
        ])
        .unwrap();
    collection
        .create_index(IndexModel {
            keys: doc! { "value": 1 },
            options: None,
        })
        .unwrap();

    metrics.enable();
    assert_matching_ids(
        &collection,
        doc! { "value": Bson::Null },
        &["missing", "null"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
        &["array"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": Bson::Undefined },
        &["undefined"],
    );
    assert_eq!(metrics.find_by_index_count(), 0);
}
