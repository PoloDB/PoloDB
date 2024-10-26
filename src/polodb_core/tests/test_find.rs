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

use polodb_core::{Result, CollectionT};
use polodb_core::bson::{doc, Document};

mod common;

use common::{
    prepare_db,
    create_file_and_return_db_with_items,
};

static TEST_SIZE: usize = 1000;

#[test]
fn test_multiple_find_one() {
    vec![
        (prepare_db("test-multiple-find-one").unwrap(), true),
    ].iter().for_each(|(db, _is_file)| {
        let metrics = db.metrics();
        metrics.enable();

        {
            let collection = db.collection("config");
            collection.insert_many(vec![
                doc! {
                    "_id": "c1",
                    "value": "c1",
                },
                doc! {
                    "_id": "c2",
                    "value": "c2",
                },
                doc! {
                    "_id": "c3",
                    "value": "c3",
                },
            ]).unwrap();

            assert_eq!(collection.count_documents().unwrap(), 3);
        }

        {
            let collection = db.collection::<Document>("config");
            collection.update_many(doc! {
                "_id": "c2"
            }, doc! {
                "$set": doc! {
                    "value": "c33",
                },
            }).unwrap();
            collection.update_many(doc! {
                "_id": "c2",
            }, doc! {
                "$set": doc! {
                    "value": "c22",
                },
            }).unwrap();
        }

        let collection = db.collection::<Document>("config");
        let doc1 = collection.find_one(doc! {
            "_id": "c1",
        }).unwrap().unwrap();

        assert_eq!(doc1.get("value").unwrap().as_str().unwrap(), "c1");

        let collection = db.collection::<Document>("config");

        let doc1 = collection.find_one(doc! {
            "_id": "c2",
        }).unwrap().unwrap();

        assert_eq!(doc1.get("value").unwrap().as_str().unwrap(), "c22");
    });
}

#[test]
fn test_find() {
    vec![
        create_file_and_return_db_with_items("test-find", TEST_SIZE),
    ].iter().for_each(|db| {
        let collection = db.collection::<Document>("test");

        let result = collection
            .find(doc! {
                "content": "3",
            })
            .run()
            .unwrap()
            .collect::<Result<Vec<Document>>>()
            .unwrap();

        assert_eq!(result.len(), 1);

        let one = result[0].clone();
        assert_eq!(one.get("content").unwrap().as_str().unwrap(), "3");
    });
}

#[test]
fn test_find_empty_collection() {
    let db = prepare_db("test-find-empty-collection").unwrap();

    {
        let collection = db.collection::<Document>("test");

        let mut cursor = collection.find(doc! {}).run().unwrap();

        assert!(!cursor.advance().unwrap());
    }

    let txn = db.start_transaction().unwrap();

    let collection = txn.collection::<Document>("test");

    let mut cursor = collection.find(doc! {}).run().unwrap();

    assert!(!cursor.advance().unwrap());
}

#[test]
fn test_find_with_empty_document() {
    vec![
        prepare_db("test-find-with-empty-document").unwrap(),
    ].iter().for_each(|db| {
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
            .find(doc! {})
            .run()
            .unwrap()
            .collect::<Result<Vec<Document>>>()
            .unwrap();
        assert_eq!(result.len(), 3);
    });
}

#[test]
fn test_not_expression() {
    vec![
        prepare_db("test-not-expression").unwrap(),
    ].iter().for_each(|db| {
        let metrics = db.metrics();
        metrics.enable();

        let col = db.collection::<Document>("teacher");

        col.insert_many(vec![
            doc! {
                "name": "David",
                "age": 33,
            },
            doc! {
                "name": "John",
                "age": 22,
            },
            doc! {
                "name": "Mary",
                "age": 18,
            },
            doc! {
                "name": "Peter",
                "age": 18,
            },
        ]).unwrap();

        let result = col
            .find(doc! {
                "age": {
                    "$not": {
                        "$eq": 18,
                    },
                },
            })
            .run()
            .unwrap()
            .collect::<Result<Vec<Document>>>().unwrap();
        assert_eq!(result.len(), 2);

        assert_eq!(result[0].get("name").unwrap().as_str().unwrap(), "David");
        assert_eq!(result[1].get("name").unwrap().as_str().unwrap(), "John");
    });
}

#[test]
fn test_find_skip() {
    let db = prepare_db("test-find-skip").unwrap();

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
            "name": "grape",
            "color": "purple",
            "shape": "round",
        },
        doc! {
            "name": "watermelon",
            "color": "green",
            "shape": "round",
        },
    ]).unwrap();

    let result = fruits
        .find(doc! {})
        .skip(2)
        .run()
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();

    assert_eq!(result.len(), 3);
}

#[test]
fn test_find_limit() {
    let db = prepare_db("test-find-limit").unwrap();

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
            "name": "grape",
            "color": "purple",
            "shape": "round",
        },
        doc! {
            "name": "watermelon",
            "color": "green",
            "shape": "round",
        },
    ]).unwrap();

    let result = fruits
        .find(doc! {})
        .limit(3)
        .run()
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].get("name").unwrap().as_str().unwrap(), "apple");
    assert_eq!(result[1].get("name").unwrap().as_str().unwrap(), "banana");
    assert_eq!(result[2].get("name").unwrap().as_str().unwrap(), "orange");

    // skip and limit
    let result = fruits
        .find(doc! {})
        .skip(2)
        .limit(2)
        .run()
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].get("name").unwrap().as_str().unwrap(), "orange");
}

#[test]
fn test_find_sort() {
    let db = prepare_db("test-find-sort").unwrap();

    let fruits = db.collection::<Document>("fruits");
    fruits.insert_many(vec![
        doc! {
            "name": "apple",
            "color": "red",
            "shape": "round",
            "weight": 1,
        },
        doc! {
            "name": "banana",
            "color": "yellow",
            "shape": "long",
            "weight": 2,
        },
        doc! {
            "name": "orange",
            "color": "orange",
            "shape": "round",
            "weight": 3,
        },
        doc! {
            "name": "grape",
            "color": "purple",
            "shape": "round",
            "weight": 4,
        },
        doc! {
            "name": "watermelon",
            "color": "green",
            "shape": "round",
            "weight": 5,
        },
    ]).unwrap();

    let result = fruits
        .find(doc! {})
        .sort(doc! {
            "weight": 1,
        })
        .run()
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 5);
    assert_eq!(result[0].get("name").unwrap().as_str().unwrap(), "apple");
    assert_eq!(result[1].get("name").unwrap().as_str().unwrap(), "banana");
    assert_eq!(result[2].get("name").unwrap().as_str().unwrap(), "orange");
}
