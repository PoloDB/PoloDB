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
use polodb_core::{Result, CollectionT, Database};
use polodb_core::test_utils::prepare_db as project_prepare_db;

#[cfg(test)]
fn prepare_db(db_name: &str) -> Result<Database> {
    let db = project_prepare_db(db_name)?;
    let fruits = db.collection::<Document>("fruits");
    fruits.insert_many(vec![
        doc! {
            "name": "apple",
            "color": "red",
            "shape": "round",
            "weight": 100,
        },
        doc! {
            "name": "banana",
            "color": "yellow",
            "shape": "long",
            "weight": 200,
        },
        doc! {
            "name": "orange",
            "color": "orange",
            "shape": "round",
            "weight": 150,
        },
        doc! {
            "name": "pear",
            "color": "yellow",
            "shape": "round",
            "weight": 120,
        },
        doc! {
            "name": "peach",
            "color": "orange",
            "shape": "round",
            "weight": 130,
        },
    ])?;
    Ok(db)
}

#[test]
fn test_aggregate_empty() {
    let db = prepare_db("test-aggregate-empty").unwrap();
    let fruits = db.collection::<Document>("fruits");

    let result = fruits
        .aggregate(vec![])
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 5);
}

#[test]
fn test_aggregate_match() {
    let db = prepare_db("test-aggregate-match").unwrap();
    let fruits = db.collection::<Document>("fruits");

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

#[test]
fn test_aggregate_limit() {
    let db = prepare_db("test-aggregate-limit").unwrap();
    let fruits = db.collection::<Document>("fruits");

    let result = fruits
        .aggregate(vec![
            doc! {
                "$limit": 2,
            }
        ])
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 2);

    assert_eq!(result[0].get("name").unwrap().as_str().unwrap(), "apple");
    assert_eq!(result[1].get("name").unwrap().as_str().unwrap(), "banana");
}

#[test]
fn test_aggregate_sort() {
    let db = prepare_db("test-aggregate-sort").unwrap();
    let fruits = db.collection::<Document>("fruits");

    let result = fruits
        .aggregate(vec![
            doc! {
                "$sort": {
                    "weight": 1,
                },
            }
        ])
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 5);
    assert_eq!(result[0].get("name").unwrap().as_str().unwrap(), "apple");
    assert_eq!(result[1].get("name").unwrap().as_str().unwrap(), "pear");
    assert_eq!(result[2].get("name").unwrap().as_str().unwrap(), "peach");
    assert_eq!(result[3].get("name").unwrap().as_str().unwrap(), "orange");
    assert_eq!(result[4].get("name").unwrap().as_str().unwrap(), "banana");

    let result = fruits
        .aggregate(vec![
            doc! {
                "$sort": {
                    "weight": -1,
                },
            }
        ])
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 5);
    assert_eq!(result[0].get("name").unwrap().as_str().unwrap(), "banana");
    assert_eq!(result[1].get("name").unwrap().as_str().unwrap(), "orange");
    assert_eq!(result[2].get("name").unwrap().as_str().unwrap(), "peach");
    assert_eq!(result[3].get("name").unwrap().as_str().unwrap(), "pear");
    assert_eq!(result[4].get("name").unwrap().as_str().unwrap(), "apple");
}

#[test]
fn test_aggregate_unset() {
    let db = prepare_db("test-aggregate-unset").unwrap();
    let fruits = db.collection::<Document>("fruits");

    let result = fruits
        .aggregate(vec![
            doc! {
                "$unset": "color",
            }
        ])
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 5);
    for doc in result {
        assert_eq!(doc.get("color"), None);
    }

    // unset multiple
    let result = fruits
        .aggregate(vec![
            doc! {
                "$unset": ["color", "shape"],
            }
        ])
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();

    assert_eq!(result.len(), 5);
    for doc in result {
        assert_eq!(doc.get("color"), None);
        assert_eq!(doc.get("shape"), None);
    }
}

#[test]
fn test_aggregate_abs() {
    let db = project_prepare_db("test-aggregate-abs").unwrap();
    let elements = db.collection::<Document>("elements");

    elements.insert_many(vec![
        doc! {
            "weight": 100,
        },
        doc! {
            "weight": -200,
        },
        doc! {
            "weight": 300,
        },
        doc! {
            "weight": -400,
        },
        doc! {
            "weight": 500,
        },
    ]).unwrap();

    let result = elements
        .aggregate(vec![
            doc! {
                "$addFields": {
                    "abs_weight": {
                        "$abs": "$weight",
                    },
                },
            }
        ])
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 5);
    for doc in result {
        let weight = doc.get("weight").unwrap().as_i32().unwrap();
        let abs_weight = doc.get("abs_weight").unwrap().as_i32().unwrap();
        assert_eq!(abs_weight, weight.abs());
    }
}
