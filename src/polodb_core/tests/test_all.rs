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
fn test_all_matches_scalars_arrays_and_every_candidate() {
    let db = prepare_db("test-all-scalars-and-arrays").unwrap();
    let collection = db.collection::<Document>("items");

    collection
        .insert_many(vec![
            doc! { "_id": "scalar_one", "value": 1 },
            doc! { "_id": "scalar_two", "value": 2 },
            doc! { "_id": "array_one", "value": [1] },
            doc! { "_id": "array_12", "value": [1, 2] },
            doc! { "_id": "array_21", "value": [2, 1] },
            doc! { "_id": "array_123", "value": [1, 2, 3] },
            doc! { "_id": "empty", "value": Bson::Array(Vec::new()) },
            doc! { "_id": "missing" },
        ])
        .unwrap();

    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [2] } },
        &["array_12", "array_123", "array_21", "scalar_two"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [1, 2] } },
        &["array_12", "array_123", "array_21"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [2, 1] } },
        &["array_12", "array_123", "array_21"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [1, 1] } },
        &[
            "array_12",
            "array_123",
            "array_21",
            "array_one",
            "scalar_one",
        ],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": Bson::Array(Vec::new()) } },
        &[],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$not": { "$all": Bson::Array(Vec::new()) } } },
        &[
            "array_12",
            "array_123",
            "array_21",
            "array_one",
            "empty",
            "missing",
            "scalar_one",
            "scalar_two",
        ],
    );
}

#[test]
fn test_all_matches_exact_and_directly_nested_arrays() {
    let db = prepare_db("test-all-nested-arrays").unwrap();
    let collection = db.collection::<Document>("items");
    let one_two = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]);
    let two_one = Bson::Array(vec![Bson::Int32(2), Bson::Int32(1)]);
    let empty_array = Bson::Array(Vec::new());

    collection
        .insert_many(vec![
            doc! { "_id": "exact", "value": one_two.clone() },
            doc! { "_id": "reordered", "value": two_one.clone() },
            doc! { "_id": "nested", "value": [one_two.clone(), Bson::Int32(3)] },
            doc! { "_id": "nested_reordered", "value": [two_one] },
            doc! { "_id": "too_deep", "value": [[one_two.clone()]] },
            doc! { "_id": "other", "value": [[1, 3]] },
            doc! { "_id": "exact_empty", "value": empty_array.clone() },
            doc! { "_id": "nested_empty", "value": [empty_array.clone()] },
            doc! { "_id": "too_deep_empty", "value": [[empty_array.clone()]] },
        ])
        .unwrap();

    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [one_two.clone()] } },
        &["exact", "nested"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [one_two, 3] } },
        &["nested"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [empty_array] } },
        &["exact_empty", "nested_empty"],
    );
}

#[test]
fn test_all_uses_equivalent_numeric_types() {
    let db = prepare_db("test-all-numeric-equality").unwrap();
    let collection = db.collection::<Document>("items");

    collection
        .insert_many(vec![
            doc! { "_id": "int32", "value": Bson::Int32(7) },
            doc! { "_id": "int64", "value": Bson::Int64(7) },
            doc! { "_id": "double", "value": Bson::Double(7.0) },
            doc! {
                "_id": "mixed",
                "value": [
                    Bson::Int32(7),
                    Bson::Double(-0.0),
                    Bson::Int64(9_007_199_254_740_992),
                ],
            },
            doc! {
                "_id": "large_next",
                "value": Bson::Int64(9_007_199_254_740_993),
            },
            doc! { "_id": "i64_min", "value": Bson::Int64(i64::MIN) },
            doc! { "_id": "i64_max", "value": Bson::Int64(i64::MAX) },
        ])
        .unwrap();

    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [Bson::Int64(7)] } },
        &["double", "int32", "int64", "mixed"],
    );
    assert_matching_ids(
        &collection,
        doc! {
            "value": {
                "$all": [Bson::Double(0.0), Bson::Double(9_007_199_254_740_992.0)]
            }
        },
        &["mixed"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [Bson::Double(9_007_199_254_740_992.0)] } },
        &["mixed"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [Bson::Double(9_223_372_036_854_775_808.0)] } },
        &[],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [Bson::Double(-9_223_372_036_854_775_808.0)] } },
        &["i64_min"],
    );
}

#[test]
fn test_all_uses_ordered_bson_equality_and_string_symbol_equivalence() {
    let db = prepare_db("test-all-bson-equality").unwrap();
    let collection = db.collection::<Document>("items");

    let ordered = doc! { "a": 1, "b": 2 };
    let reversed = doc! { "b": 2, "a": 1 };
    let db_ref = doc! { "$ref": "targets", "$id": ObjectId::new() };
    let ref_literal = doc! { "$ref": "targets" };
    let ref_operator_literal = doc! { "$ref": "targets", "$gt": 1 };
    let id_first_literal = doc! { "$id": 7, "$ref": "targets" };
    let db_first_literal = doc! { "$db": "app", "$ref": "targets", "$id": 7 };

    collection
        .insert_many(vec![
            doc! { "_id": "ordered", "value": ordered.clone() },
            doc! { "_id": "reversed", "value": reversed.clone() },
            doc! { "_id": "array_ordered", "value": [ordered.clone()] },
            doc! { "_id": "db_ref", "value": db_ref.clone() },
            doc! { "_id": "ref_literal", "value": ref_literal.clone() },
            doc! { "_id": "ref_operator_literal", "value": ref_operator_literal.clone() },
            doc! { "_id": "id_first_literal", "value": id_first_literal.clone() },
            doc! { "_id": "db_first_literal", "value": db_first_literal.clone() },
            doc! { "_id": "string", "value": "alpha" },
            doc! { "_id": "symbol", "value": Bson::Symbol("alpha".into()) },
            doc! {
                "_id": "symbol_string_array",
                "value": [Bson::Symbol("alpha".into()), Bson::String("beta".into())],
            },
        ])
        .unwrap();

    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [Bson::Document(ordered)] } },
        &["array_ordered", "ordered"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [Bson::Document(reversed)] } },
        &["reversed"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [Bson::Document(db_ref)] } },
        &["db_ref"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [Bson::Document(ref_literal)] } },
        &["ref_literal"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [Bson::Document(ref_operator_literal)] } },
        &["ref_operator_literal"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [Bson::Document(id_first_literal)] } },
        &["id_first_literal"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [Bson::Document(db_first_literal)] } },
        &["db_first_literal"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": ["alpha"] } },
        &["string", "symbol", "symbol_string_array"],
    );
    assert_matching_ids(
        &collection,
        doc! {
            "value": {
                "$all": [Bson::Symbol("alpha".into()), Bson::Symbol("beta".into())]
            }
        },
        &["symbol_string_array"],
    );
}

#[test]
fn test_all_regex_candidates_match_strings_and_are_validated_eagerly() {
    let db = prepare_db("test-all-regex").unwrap();
    let collection = db.collection::<Document>("items");

    let alice_regex = Regex {
        pattern: "^ali".into(),
        options: "i".into(),
    };
    let carol_regex = Regex {
        pattern: "^car".into(),
        options: "i".into(),
    };

    collection
        .insert_many(vec![
            doc! { "_id": "scalar", "value": "Alice" },
            doc! { "_id": "array", "value": ["Alice", "Carol"] },
            doc! { "_id": "symbol", "value": Bson::Symbol("ALICE".into()) },
            doc! {
                "_id": "symbol_array",
                "value": [Bson::Symbol("Alice".into()), Bson::Symbol("Carol".into())],
            },
            doc! {
                "_id": "stored_regex",
                "value": Bson::RegularExpression(alice_regex.clone()),
            },
            doc! {
                "_id": "regex_array",
                "value": [Bson::RegularExpression(alice_regex.clone())],
            },
            doc! { "_id": "other", "value": "David" },
        ])
        .unwrap();

    assert_matching_ids(
        &collection,
        doc! {
            "value": {
                "$all": [Bson::RegularExpression(alice_regex.clone())]
            }
        },
        &[
            "array",
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
                "$all": [
                    Bson::RegularExpression(alice_regex),
                    Bson::RegularExpression(carol_regex),
                ]
            }
        },
        &["array", "symbol_array"],
    );

    assert!(collection
        .find(doc! {
            "value": {
                "$all": ["does-not-match", Bson::RegularExpression(Regex {
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
                "$all": [Bson::RegularExpression(Regex {
                    pattern: "Alice".into(),
                    options: "q".into(),
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
                "$all": [Bson::RegularExpression(Regex {
                    pattern: "[".into(),
                    options: "".into(),
                })]
            }
        })
        .run()
        .is_err());
}

#[test]
fn test_all_and_not_handle_null_and_missing_fields() {
    let db = prepare_db("test-all-null-and-missing").unwrap();
    let collection = db.collection::<Document>("items");

    collection
        .insert_many(vec![
            doc! { "_id": "null", "value": Bson::Null },
            doc! { "_id": "missing" },
            doc! { "_id": "array_null", "value": [Bson::Null] },
            doc! { "_id": "array_null_one", "value": [Bson::Null, Bson::Int32(1)] },
            doc! { "_id": "one", "value": 1 },
            doc! { "_id": "array_one", "value": [1] },
        ])
        .unwrap();

    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [Bson::Null] } },
        &["array_null", "array_null_one", "missing", "null"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$not": { "$all": [Bson::Null] } } },
        &["array_one", "one"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [Bson::Null, Bson::Int64(1)] } },
        &["array_null_one"],
    );
    assert_matching_ids(
        &collection,
        doc! { "value": { "$not": { "$all": [Bson::Null, 1] } } },
        &["array_null", "array_one", "missing", "null", "one"],
    );
}

#[test]
fn test_all_handles_dotted_document_paths() {
    let db = prepare_db("test-all-dotted-paths").unwrap();
    let collection = db.collection::<Document>("items");

    collection
        .insert_many(vec![
            doc! { "_id": "ada", "profile": { "names": ["Ada", "Lovelace"] } },
            doc! { "_id": "grace", "profile": { "names": "Grace" } },
            doc! { "_id": "explicit_null", "profile": { "names": Bson::Null } },
            doc! { "_id": "leaf_missing", "profile": {} },
            doc! { "_id": "middle_missing" },
            doc! { "_id": "middle_scalar", "profile": "legacy" },
        ])
        .unwrap();

    assert_matching_ids(
        &collection,
        doc! { "profile.names": { "$all": ["Ada", "Lovelace"] } },
        &["ada"],
    );
    assert_matching_ids(
        &collection,
        doc! { "profile.names": { "$all": ["Grace"] } },
        &["grace"],
    );
    assert_matching_ids(
        &collection,
        doc! { "profile.names": { "$all": [Bson::Null] } },
        &[
            "explicit_null",
            "leaf_missing",
            "middle_missing",
            "middle_scalar",
        ],
    );
    assert_matching_ids(
        &collection,
        doc! { "profile.names": { "$not": { "$all": ["Ada"] } } },
        &[
            "explicit_null",
            "grace",
            "leaf_missing",
            "middle_missing",
            "middle_scalar",
        ],
    );
}

#[test]
fn test_all_validates_operands_and_rejects_operator_documents() {
    let db = prepare_db("test-all-invalid-operands").unwrap();
    let collection = db.collection::<Document>("items");
    let literal = doc! { "x": 1, "$regex": "literal" };

    collection
        .insert_many(vec![
            doc! { "_id": "one", "value": "Alice" },
            doc! { "_id": "literal", "value": literal.clone() },
        ])
        .unwrap();

    assert!(collection
        .find(doc! { "value": { "$all": "Alice" } })
        .run()
        .is_err());
    assert!(collection
        .find(doc! { "value": { "$all": Bson::Document(doc! { "x": 1 }) } })
        .run()
        .is_err());
    assert!(collection
        .find(doc! {
            "value": {
                "$all": [Bson::Document(doc! { "$gt": 1 })]
            }
        })
        .run()
        .is_err());
    assert!(collection
        .find(doc! {
            "value": {
                "$all": [Bson::Document(doc! { "$regex": "^Ali" })]
            }
        })
        .run()
        .is_err());
    assert!(collection
        .find(doc! {
            "value": {
                "$all": [Bson::Document(doc! { "$elemMatch": { "x": 1 } })]
            }
        })
        .run()
        .is_err());
    assert!(collection
        .find(doc! {
            "value": {
                "$all": [Bson::Document(doc! {
                    "$gt": 1,
                    "$ref": "targets",
                    "$id": 7,
                })]
            }
        })
        .run()
        .is_err());

    assert_matching_ids(
        &collection,
        doc! { "value": { "$all": [Bson::Document(literal)] } },
        &["literal"],
    );
}
