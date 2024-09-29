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

use polodb_core::{CollectionT, IndexModel, IndexOptions, Result};
use bson::{doc, Document};
use crate::common::prepare_db;

mod common;

#[test]
fn test_create_multi_keys_index() {
    let db = prepare_db("test-create-multi-keys-index").unwrap();
    let col = db.collection::<Document>("teacher");
    let result = col.create_index(IndexModel {
        keys: doc! {
            "age": 1,
            "name": 1,
        },
        options: None,
    });
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("only support single field indexes currently"));
}

#[test]
fn test_create_reverse_order_index() {
    let db = prepare_db("test-create-reverse-order-index").unwrap();
    let col = db.collection::<Document>("teacher");
    let result = col.create_index(IndexModel {
        keys: doc! {
            "age": -1,
        },
        options: None,
    });
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("only support ascending order index currently"));
}

#[test]
fn test_create_index() {
    vec![
        prepare_db("test-create-index").unwrap(),
    ].iter().for_each(|db| {
        let col = db.collection("teacher");

        col.create_index(IndexModel {
            keys: doc! {
                "age": 1,
            },
            options: None,
        }).unwrap();

        col.insert_one(doc! {
            "name": "David",
            "age": 33,
        }).unwrap();
    });
}

#[test]
fn test_create_index_with_data() {
    vec![
        prepare_db("test-create-index-with-data").unwrap(),
    ].iter().for_each(|db| {
        let metrics = db.metrics();
        metrics.enable();

        let col = db.collection::<Document>("teacher");

        col.insert_one(doc! {
            "name": "David",
            "age": 33,
        }).unwrap();

        col.create_index(IndexModel {
            keys: doc! {
                "age": 1,
            },
            options: None,
        }).unwrap();

        let doc = col.find_one(doc! {
            "age": 33
        }).unwrap().unwrap();

        assert_eq!(doc.get_str("name").unwrap(), "David");

        assert_eq!(metrics.find_by_index_count(), 1);
    });
}

#[test]
fn test_find_by_index() {
    vec![
        prepare_db("test-find-by-index").unwrap(),
    ].iter().for_each(|db| {
        let metrics = db.metrics();
        metrics.enable();

        let col = db.collection::<Document>("teacher");

        col.create_index(IndexModel {
            keys: doc! {
                "age": 1,
            },
            options: None,
        }).unwrap();

        col.insert_one(doc! {
            "name": "David",
            "age": 33,
        }).unwrap();

        let doc = col.find_one(doc! {
            "age": 33
        }).unwrap().unwrap();

        assert_eq!(doc.get_str("name").unwrap(), "David");

        assert_eq!(metrics.find_by_index_count(), 1);
    });
}

#[test]
fn test_index_order() {
    vec![
        prepare_db("test-index-order").unwrap(),
    ].iter().for_each(|db| {
        let metrics = db.metrics();
        metrics.enable();

        let col = db.collection::<Document>("teacher");

        col.create_index(IndexModel {
            keys: doc! {
                "age": 1,
            },
            options: None,
        }).unwrap();

        col.insert_many(vec![
            doc! {
                "name": "David",
                "age": 23,
            },
            doc! {
                "name": "John",
            },
            doc! {
                "name": "Dick",
                "age": 23,
            }
        ]).unwrap();

        let people23 = col
            .find(doc! {
                "age": 23
            })
            .run()
            .unwrap()
            .collect::<Result<Vec<Document>>>()
            .unwrap();

        assert_eq!(people23.len(), 2);
    });
}

#[test]
fn test_create_unique_index() {
    vec![
        prepare_db("test-create-unique-index").unwrap(),
    ].iter().for_each(|db| {
        let col = db.collection("teacher");

        col.create_index(IndexModel {
            keys: doc! {
                "name": 1,
            },
            options: Some(IndexOptions{
                unique: Some(true),
                ..Default::default()
            }),
        }).unwrap();

        col.insert_one(doc! {
            "name": "David",
            "age": 33,
        }).unwrap();

        let result = col.insert_one(doc! {
            "name": "David",
            "age": 33,
        });

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("duplicate key error"));
    });
}

#[test]
fn test_update_with_index() {
    vec![
        prepare_db("test-update-with-index").unwrap(),
    ].iter().for_each(|db| {
        let metrics = db.metrics();
        metrics.enable();

        let col = db.collection("teacher");

        col.create_index(IndexModel {
            keys: doc! {
                "age": 1,
            },
            options: None,
        }).unwrap();

        col.insert_one(doc! {
            "name": "David",
            "age": 33,
        }).unwrap();

        col.update_many(doc! {
            "age": 33,
        }, doc! {
            "$set": {
                "age": 34,
            },
        }).unwrap();

        let doc = col.find_one(doc! {
            "age": 34
        }).unwrap().unwrap();

        assert_eq!(doc.get_str("name").unwrap(), "David");

        assert_eq!(metrics.find_by_index_count(), 1);
    });
}

#[test]
fn test_delete_with_index() {
    vec![
        prepare_db("test-delete-with-index").unwrap(),
    ].iter().for_each(|db| {
        let metrics = db.metrics();
        metrics.enable();

        let col = db.collection::<Document>("teacher");

        col.create_index(IndexModel {
            keys: doc! {
                "age": 1,
            },
            options: None,
        }).unwrap();

        col.insert_one(doc! {
            "name": "David",
            "age": 33,
        }).unwrap();

        let result = col.delete_many(doc! {
            "age": 33,
        }).unwrap();

        assert_eq!(result.deleted_count, 1);

        let count = col.count_documents().unwrap();
        assert_eq!(count, 0);

        let result = col.find_one(doc! {
            "age": 33
        }).unwrap();
        assert_eq!(result, None);

        assert!(col.find_one(doc! {
            "age": 33
        }).unwrap().is_none());
    });
}

#[test]
fn test_drop_index() {
    vec![
        prepare_db("test-drop-index").unwrap(),
    ].iter().for_each(|db| {
        let metrics = db.metrics();
        metrics.enable();

        let col = db.collection::<Document>("teacher");

        col.create_index(IndexModel {
            keys: doc! {
                "age": 1,
            },
            options: None,
        }).unwrap();

        col.insert_one(doc! {
            "name": "David",
            "age": 33,
        }).unwrap();

        col.drop_index("age_1").unwrap();

        {
            let doc = col.find_one(doc! {
                "age": 33,
            }).unwrap().unwrap();

            assert_eq!(doc.get_str("name").unwrap(), "David");
        }

        assert_eq!(metrics.find_by_index_count(), 0);
    });
}

#[test]
fn test_issue_171() {
    let db = prepare_db("test-issue-171").unwrap();

    let col = db.collection::<Document>("teacher");

    col.create_index(IndexModel {
        keys: doc! {
                "name": 1,
            },
        options: None,
    }).unwrap();

    col.insert_one(doc! {
        "name": "David",
        "age": 33,
    }).unwrap();

    col.insert_one(doc! {
        "name": "Harry",
        "age": 32,
    }).unwrap();

    let doc = col.find_one(doc! {
        "name": "David"
    }).unwrap().unwrap();

    assert_eq!(doc.get_str("name").unwrap(), "David");

    let docs:Vec<Document> = col.find(doc! {
        "name": "David"
    }).run().unwrap().into_iter().map(|cc| cc.unwrap()).collect();

    assert_eq!(docs.len(), 1);

    assert_eq!(docs[0].get_str("name").unwrap(), "David");
}
