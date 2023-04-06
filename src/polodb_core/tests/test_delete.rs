/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use polodb_core::Database;
use polodb_core::bson::{doc, Document};

mod common;

use common::prepare_db;

#[test]
fn test_delete_one() {
    vec![
        prepare_db("test-update-one").unwrap(),
        Database::open_memory().unwrap(),
    ].iter().for_each(|db| {
        let collection = db.collection::<Document>("test");

        let result = collection.insert_many(vec![
            doc! {
                    "name": "Vincent",
                    "age": 17,
                },
            doc! {
                    "name": "Vincent",
                    "age": 18,
                },
        ]).unwrap();

        assert_eq!(result.inserted_ids.len(), 2);

        let result = collection.delete_one(doc! {
                "name": "Vincent",
            }).unwrap();

        assert_eq!(result.deleted_count, 1);

        let remain = collection.count_documents().unwrap();
        assert_eq!(remain, 1);
    });
}

#[test]
fn test_one_delete_item() {
    vec![
        prepare_db("test-delete-item").unwrap(),
        Database::open_memory().unwrap(),
    ].iter().for_each(|db| {
        let collection = db.collection::<Document>("test");

        let mut doc_collection  = vec![];

        for i in 0..100 {
            let content = i.to_string();

            let new_doc = doc! {
                    "content": content,
                };

            doc_collection.push(new_doc);
        }
        let result = collection.insert_many(&doc_collection).unwrap();

        let third_key = result.inserted_ids.get(&3).unwrap();
        let delete_doc = doc! {
                "_id": third_key.clone(),
            };
        assert_eq!(collection.delete_many(delete_doc.clone()).unwrap().deleted_count, 1);
        assert_eq!(collection.delete_many(delete_doc).unwrap().deleted_count, 0);
    });
}

#[test]
fn test_delete_many() {
    vec![
        prepare_db("test-delete-many").unwrap(),
        Database::open_memory().unwrap(),
    ].iter().for_each(|db| {
        let metrics = db.metrics();
        metrics.enable();

        let collection = db.collection::<Document>("test");

        let mut doc_collection  = vec![];

        for i in 0..1000 {
            let content = i.to_string();
            let new_doc = doc! {
                    "_id": i,
                    "content": content,
                };
            doc_collection.push(new_doc);
        }
        collection.insert_many(&doc_collection).unwrap();

        collection.delete_many(doc! {}).unwrap();

        assert_eq!(collection.count_documents().unwrap(), 0);
    });
}

#[test]
fn test_delete_all_items() {
    vec![
        prepare_db("test-delete-all-items").unwrap(),
        Database::open_memory().unwrap(),
    ].iter().for_each(|db| {
        let metrics = db.metrics();
        metrics.enable();

        let collection = db.collection::<Document>("test");

        let mut doc_collection  = vec![];

        for i in 0..1000 {
            let content = i.to_string();
            let new_doc = doc! {
                    "_id": i,
                    "content": content,
                };
            doc_collection.push(new_doc);
        }
        collection.insert_many(&doc_collection).unwrap();

        let mut counter = 0;
        for doc in &doc_collection {
            let key = doc.get("_id").unwrap();
            let deleted_result = collection.delete_many(doc!{
                "_id": key.clone(),
            }).expect(format!("delete error: {}", counter).as_str());
            assert!(deleted_result.deleted_count > 0, "delete nothing with key: {}, count: {}", key, deleted_result.deleted_count);
            let find_doc = doc! {
                "_id": key.clone(),
            };
            let result = collection.find_many(find_doc).unwrap();
            assert_eq!(result.len(), 0, "item with key: {}", key);
            counter += 1;
        }
    });
}
