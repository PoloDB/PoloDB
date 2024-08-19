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

use bson::{doc, Document};
use polodb_core::{Result, CollectionT};
use polodb_core::test_utils::prepare_db;

#[test]
fn test_aggregate_empty() {
    let db = prepare_db("test-aggregate-empty").unwrap();
    let fruits = db.collection::<Document>("fruits");
    fruits.insert_many(vec![
        doc! {
            "name": "apple",
            "color": "red",
            "shape": "round",
        },
        doc! {
            "name": "banana",
            "color": "yellow",
            "shape": "long",
        },
        doc! {
            "name": "orange",
            "color": "orange",
            "shape": "round",
        },
    ]).unwrap();

    let result = fruits
        .aggregate(vec![])
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 3);
}

#[test]
fn test_aggregate_match() {
    let db = prepare_db("test-aggregate-match").unwrap();
    let fruits = db.collection::<Document>("fruits");
    fruits.insert_many(vec![
        doc! {
            "name": "apple",
            "color": "red",
            "shape": "round",
        },
        doc! {
            "name": "banana",
            "color": "yellow",
            "shape": "long",
        },
        doc! {
            "name": "orange",
            "color": "orange",
            "shape": "round",
        },
        doc! {
            "name": "pear",
            "color": "yellow",
            "shape": "round",
        },
        doc! {
            "name": "peach",
            "color": "orange",
            "shape": "round",
        },
    ]).unwrap();

    let result = fruits
        .aggregate(vec![
            doc! {
                "$match": {
                    "color": "yellow",
                },
            }
        ])
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 2);

    assert_eq!(result[0].get("name").unwrap().as_str().unwrap(), "banana");
    assert_eq!(result[1].get("name").unwrap().as_str().unwrap(), "pear");
}

#[test]
fn test_aggregate_count() {
    let db = prepare_db("test-aggregate-count").unwrap();
    let fruits = db.collection::<Document>("fruits");
    fruits.insert_many(vec![
        doc! {
            "name": "apple",
            "color": "red",
            "shape": "round",
        },
        doc! {
            "name": "banana",
            "color": "yellow",
            "shape": "long",
        },
        doc! {
            "name": "orange",
            "color": "orange",
            "shape": "round",
        },
        doc! {
            "name": "pear",
            "color": "yellow",
            "shape": "round",
        },
        doc! {
            "name": "peach",
            "color": "orange",
            "shape": "round",
        },
    ]).unwrap();

    let result = fruits
        .aggregate(vec![
            doc! {
                "$match": {
                    "color": "yellow",
                },
            },
            doc! {
                "$count": "count",
            }
        ])
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("count").unwrap().as_i64().unwrap(), 2);

    let result = fruits
        .aggregate(vec![
            doc! {
                "$count": "count",
            }
        ])
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("count").unwrap().as_i64().unwrap(), 5);

    let result = fruits
        .aggregate(vec![
            doc! {
                "$match": {},
            },
            doc! {
                "$count": "count",
            }
        ])
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("count").unwrap().as_i64().unwrap(), 5);
}

#[test]
fn test_aggregate_skip() {
    let db = prepare_db("test-aggregate-skip").unwrap();
    let fruits = db.collection::<Document>("fruits");
    fruits.insert_many(vec![
        doc! {
            "name": "apple",
            "color": "red",
            "shape": "round",
        },
        doc! {
            "name": "banana",
            "color": "yellow",
            "shape": "long",
        },
        doc! {
            "name": "orange",
            "color": "orange",
            "shape": "round",
        },
        doc! {
            "name": "pear",
            "color": "yellow",
            "shape": "round",
        },
        doc! {
            "name": "peach",
            "color": "orange",
            "shape": "round",
        },
    ]).unwrap();

    let result = fruits
        .aggregate(vec![
            doc! {
                "$skip": 2,
            }
        ])
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 3);

    assert_eq!(result[0].get("name").unwrap().as_str().unwrap(), "orange");
    assert_eq!(result[1].get("name").unwrap().as_str().unwrap(), "pear");
    assert_eq!(result[2].get("name").unwrap().as_str().unwrap(), "peach");
}
