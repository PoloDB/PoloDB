/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use bson::{doc, Document};
use polodb_core::{Database, Result};
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
}
