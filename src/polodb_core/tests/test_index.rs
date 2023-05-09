/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use polodb_core::{Database, IndexModel, IndexOptions};
use bson::{doc, Document};
use crate::common::prepare_db;

mod common;

#[test]
fn test_create_multi_keys_index() {
    let db = Database::open_memory().unwrap();
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
    let db = Database::open_memory().unwrap();
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
        Database::open_memory().unwrap(),
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
        Database::open_memory().unwrap(),
    ].iter().for_each(|db| {
        let metrics = db.metrics();
        metrics.enable();

        let col = db.collection("teacher");

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

        let mut cursor = col.find(doc! {
            "age": 33
        }).unwrap();

        assert!(cursor.advance().unwrap());

        let doc = cursor.deserialize_current().unwrap();
        assert_eq!(doc.get_str("name").unwrap(), "David");

        assert_eq!(metrics.find_by_index_count(), 1);
    });
}

#[test]
fn test_find_by_index() {
    vec![
        prepare_db("test-find-by-index").unwrap(),
        Database::open_memory().unwrap(),
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

        let mut cursor = col.find(doc! {
            "age": 33
        }).unwrap();

        assert!(cursor.advance().unwrap());

        let doc = cursor.deserialize_current().unwrap();
        assert_eq!(doc.get_str("name").unwrap(), "David");

        assert_eq!(metrics.find_by_index_count(), 1);
    });
}

#[test]
fn test_create_unique_index() {
    vec![
        prepare_db("test-create-unique-index").unwrap(),
        Database::open_memory().unwrap(),
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
        Database::open_memory().unwrap(),
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

        let mut cursor = col.find(doc! {
            "age": 34
        }).unwrap();

        assert!(cursor.advance().unwrap());

        let doc = cursor.deserialize_current().unwrap();
        assert_eq!(doc.get_str("name").unwrap(), "David");

        assert_eq!(metrics.find_by_index_count(), 2);
    });
}

#[test]
fn test_drop_index() {
    vec![
        prepare_db("test-drop-index").unwrap(),
        Database::open_memory().unwrap(),
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

        col.drop_index("age_1").unwrap();

        {
            let mut cursor = col.find(doc! {
                "age": 33,
            }).unwrap();

            assert!(cursor.advance().unwrap());

            let doc = cursor.deserialize_current().unwrap();
            assert_eq!(doc.get_str("name").unwrap(), "David");
        }

        assert_eq!(metrics.find_by_index_count(), 0);
    });
}
