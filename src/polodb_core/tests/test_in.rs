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

use bson::oid::ObjectId;
use bson::{doc, Bson, Document, Regex};
use polodb_core::{Collection, CollectionT};

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
fn test_in_matches_scalars_and_equivalent_numeric_types() {
    let db = prepare_db("test-in-scalars-and-numbers").unwrap();
    let collection = db.collection::<Document>("items");

    collection
        .insert_many(vec![
            doc! { "_id": "int32", "value": Bson::Int32(7) },
            doc! { "_id": "int64", "value": Bson::Int64(7) },
            doc! { "_id": "double", "value": Bson::Double(7.0) },
            doc! { "_id": "negative_zero", "value": Bson::Double(-0.0) },
            doc! { "_id": "large_exact", "value": Bson::Int64(9_007_199_254_740_992) },
            doc! { "_id": "large_next", "value": Bson::Int64(9_007_199_254_740_993) },
            doc! { "_id": "i64_min", "value": Bson::Int64(i64::MIN) },
            doc! { "_id": "i64_max", "value": Bson::Int64(i64::MAX) },
            doc! { "_id": "other", "value": 8 },
            doc! { "_id": "string", "value": "seven" },
            doc! { "_id": "symbol", "value": Bson::Symbol("seven".into()) },
            doc! { "_id": "symbol_array", "value": [Bson::Symbol("seven".into())] },
        ])
        .unwrap();

    assert_matching_ids(
        &collection,
        doc! { "value": { "$in": [Bson::Int64(7)] } },
        &["double", "int32", "int64"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$in": ["seven", 8] } },
        &["other", "string", "symbol", "symbol_array"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$in": [Bson::Symbol("seven".into())] } },
        &["string", "symbol", "symbol_array"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$in": [0] } },
        &["negative_zero"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$in": [Bson::Double(9_007_199_254_740_992.0)] } },
        &["large_exact"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$in": [Bson::Double(9_223_372_036_854_775_808.0)] } },
        &[],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$in": [Bson::Double(-9_223_372_036_854_775_808.0)] } },
        &["i64_min"],
    );
}

#[test]
fn test_in_matches_array_elements_exact_arrays_and_nested_arrays() {
    let db = prepare_db("test-in-array-semantics").unwrap();
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
        doc! { "value": { "$in": [2] } },
        &["array", "reordered", "scalar"],
    );
    assert_matching_ids(
        &collection,
        doc! {
            "value": {
                "$in": [Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])]
            }
        },
        &["array", "nested"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$in": [Bson::Array(Vec::new())] } },
        &["empty", "nested_empty"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$nin": [2] } },
        &["empty", "nested", "nested_empty"],
    );
}

#[test]
fn test_in_document_equality_is_recursive_and_order_sensitive() {
    let db = prepare_db("test-in-document-equality").unwrap();
    let collection = db.collection::<Document>("items");

    let ordered = doc! { "a": 1, "b": 2 };
    let reversed = doc! { "b": 2, "a": 1 };
    let db_ref = doc! { "$ref": "targets", "$id": ObjectId::new() };

    collection
        .insert_many(vec![
            doc! { "_id": "ordered", "value": ordered.clone() },
            doc! { "_id": "reversed", "value": reversed.clone() },
            doc! { "_id": "array_ordered", "value": [ordered.clone()] },
            doc! { "_id": "different", "value": { "a": 1, "b": 3 } },
            doc! { "_id": "db_ref", "value": db_ref.clone() },
        ])
        .unwrap();

    assert_matching_ids(
        &collection,
        doc! { "value": { "$in": [Bson::Document(ordered)] } },
        &["array_ordered", "ordered"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$in": [Bson::Document(reversed)] } },
        &["reversed"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$in": [Bson::Document(db_ref)] } },
        &["db_ref"],
    );
}

#[test]
fn test_in_regex_matches_only_strings_in_scalars_and_arrays() {
    let db = prepare_db("test-in-regex").unwrap();
    let collection = db.collection::<Document>("items");

    let alice_regex = Regex {
        pattern: "^ali".into(),
        options: "i".into(),
    };

    collection
        .insert_many(vec![
            doc! { "_id": "scalar", "value": "Alice" },
            doc! { "_id": "array", "value": ["Bob", "Carol"] },
            doc! { "_id": "symbol", "value": Bson::Symbol("ALICE".into()) },
            doc! { "_id": "symbol_array", "value": [Bson::Symbol("Alice".into())] },
            doc! { "_id": "stored_regex", "value": Bson::RegularExpression(alice_regex.clone()) },
            doc! { "_id": "regex_array", "value": [Bson::RegularExpression(alice_regex.clone())] },
            doc! { "_id": "number", "value": 123 },
            doc! { "_id": "other", "value": "David" },
        ])
        .unwrap();

    assert_matching_ids(
        &collection,
        doc! {
            "value": {
                "$in": [Bson::RegularExpression(alice_regex)]
            }
        },
        &[
            "regex_array",
            "scalar",
            "stored_regex",
            "symbol",
            "symbol_array",
        ],
    );
    assert_matching_ids(
        &collection,
        doc! {
            "value": {
                "$in": [Bson::RegularExpression(Regex {
                    pattern: "^Car".into(),
                    options: "".into(),
                })]
            }
        },
        &["array"],
    );

    assert!(collection
        .find(doc! {
            "value": {
                "$in": [Bson::RegularExpression(Regex {
                    pattern: "[".into(),
                    options: "".into(),
                })]
            }
        })
        .run()
        .is_err());
    assert!(collection
        .find(doc! {
            "value": {
                "$in": ["Alice", Bson::RegularExpression(Regex {
                    pattern: "[".into(),
                    options: "".into(),
                })]
            }
        })
        .run()
        .is_err());

    db.create_collection("empty").unwrap();
    let empty_collection = db.collection::<Document>("empty");
    assert!(empty_collection
        .find(doc! {
            "value": {
                "$in": [Bson::RegularExpression(Regex {
                    pattern: "[".into(),
                    options: "".into(),
                })]
            }
        })
        .run()
        .is_err());
}

#[test]
fn test_in_and_nin_empty_candidate_arrays() {
    let db = prepare_db("test-in-empty-candidates").unwrap();
    let collection = db.collection::<Document>("items");

    collection
        .insert_many(vec![
            doc! { "_id": "scalar", "value": 1 },
            doc! { "_id": "array", "value": [1, 2] },
            doc! { "_id": "null", "value": Bson::Null },
            doc! { "_id": "missing" },
        ])
        .unwrap();

    assert_matching_ids(
        &collection,
        doc! { "value": { "$in": Bson::Array(Vec::new()) } },
        &[],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$nin": Bson::Array(Vec::new()) } },
        &["array", "missing", "null", "scalar"],
    );
}

#[test]
fn test_in_nin_and_not_handle_null_and_missing_fields() {
    let db = prepare_db("test-in-null-and-missing").unwrap();
    let collection = db.collection::<Document>("items");

    collection
        .insert_many(vec![
            doc! { "_id": "null", "value": Bson::Null },
            doc! { "_id": "missing" },
            doc! { "_id": "one", "value": 1 },
            doc! { "_id": "two", "value": 2 },
        ])
        .unwrap();

    assert_matching_ids(
        &collection,
        doc! { "value": { "$in": [Bson::Null] } },
        &["missing", "null"],
    );
    assert_matching_ids(&collection, doc! { "value": { "$in": [1] } }, &["one"]);
    assert_matching_ids(
        &collection,
        doc! { "value": { "$nin": [1] } },
        &["missing", "null", "two"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$nin": [Bson::Null] } },
        &["one", "two"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$not": { "$in": [1] } } },
        &["missing", "null", "two"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$not": { "$in": [Bson::Null] } } },
        &["one", "two"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$not": { "$nin": [1] } } },
        &["one"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$not": { "$nin": [Bson::Null] } } },
        &["missing", "null"],
    );
}

#[test]
fn test_in_handles_missing_dotted_paths() {
    let db = prepare_db("test-in-dotted-paths").unwrap();
    let collection = db.collection::<Document>("items");

    collection
        .insert_many(vec![
            doc! { "_id": "ada", "profile": { "name": "Ada" } },
            doc! { "_id": "grace", "profile": { "name": "Grace" } },
            doc! { "_id": "leaf_missing", "profile": {} },
            doc! { "_id": "middle_missing" },
            doc! { "_id": "middle_scalar", "profile": "legacy" },
        ])
        .unwrap();

    assert_matching_ids(
        &collection,
        doc! { "profile.name": { "$in": ["Ada"] } },
        &["ada"],
    );
    assert_matching_ids(
        &collection,
        doc! { "profile.name": { "$in": [Bson::Null] } },
        &["leaf_missing", "middle_missing", "middle_scalar"],
    );
    assert_matching_ids(
        &collection,
        doc! { "profile.name": { "$nin": ["Ada"] } },
        &["grace", "leaf_missing", "middle_missing", "middle_scalar"],
    );
}

#[test]
fn test_in_negation_composes_with_other_predicates() {
    let db = prepare_db("test-in-negation-composition").unwrap();
    let collection = db.collection::<Document>("items");

    collection
        .insert_many(vec![
            doc! { "_id": "included_missing", "kind": "included" },
            doc! { "_id": "included_one", "kind": "included", "value": 1 },
            doc! { "_id": "included_two", "kind": "included", "value": 2 },
            doc! { "_id": "excluded_two", "kind": "excluded", "value": 2 },
        ])
        .unwrap();

    assert_matching_ids(
        &collection,
        doc! { "kind": "included", "value": { "$nin": [1] } },
        &["included_missing", "included_two"],
    );
    assert_matching_ids(
        &collection,
        doc! { "kind": "included", "value": { "$not": { "$in": [1] } } },
        &["included_missing", "included_two"],
    );
}

#[test]
fn test_in_and_nin_validate_operands_and_operator_documents() {
    let db = prepare_db("test-in-invalid-operands").unwrap();
    let collection = db.collection::<Document>("items");
    collection
        .insert_one(doc! { "_id": "one", "value": "Alice" })
        .unwrap();

    assert!(collection
        .find(doc! { "value": { "$in": "Alice" } })
        .run()
        .is_err());
    assert!(collection
        .find(doc! { "value": { "$nin": 1 } })
        .run()
        .is_err());
    assert!(collection
        .find(doc! {
            "value": {
                "$in": [Bson::Document(doc! { "$regex": "^Ali" })]
            }
        })
        .run()
        .is_err());
    assert!(collection
        .find(doc! {
            "value": {
                "$in": [Bson::Document(doc! { "$gt": 1 })]
            }
        })
        .run()
        .is_err());
    assert!(collection
        .find(doc! {
            "value": {
                "$in": [Bson::Document(doc! { "$ref": "targets" })]
            }
        })
        .run()
        .is_err());

    collection
        .insert_one(doc! {
            "_id": "literal",
            "value": Bson::Document(doc! { "x": 1, "$regex": "literal" }),
        })
        .unwrap();
    assert_matching_ids(
        &collection,
        doc! {
            "value": {
                "$in": [Bson::Document(doc! { "x": 1, "$regex": "literal" })]
            }
        },
        &["literal"],
    );
}
