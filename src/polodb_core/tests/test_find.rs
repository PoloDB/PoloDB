/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use polodb_core::{Database, DbResult};
use polodb_core::bson::{doc, Document};

mod common;

use common::{
    prepare_db,
    create_memory_and_return_db_with_items,
    create_file_and_return_db_with_items,
};

static TEST_SIZE: usize = 1000;

#[test]
fn test_multiple_find_one() {
    vec![
        (prepare_db("test-multiple-find-one").unwrap(), true),
        (Database::open_memory().unwrap(), false),
    ].iter().for_each(|(db, _is_file)| {
        let metrics = db.metrics();
        metrics.enable();

        {
            let collection = db.collection("config");
            let doc1 = doc! {
                    "_id": "c1",
                    "value": "c1",
                };
            collection.insert_one(doc1).unwrap();

            let doc2 = doc! {
                    "_id": "c2",
                    "value": "c2",
                };
            collection.insert_one(doc2).unwrap();

            let doc2 = doc! {
                    "_id": "c3",
                    "value": "c3",
                };
            collection.insert_one(doc2).unwrap();

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
        let mut doc1_cursor = collection.find(doc! {
            "_id": "c1",
        }).unwrap();

        assert!(doc1_cursor.advance().unwrap());

        let doc1 = doc1_cursor.deserialize_current().unwrap();

        assert_eq!(doc1.get("value").unwrap().as_str().unwrap(), "c1");

        let collection = db.collection::<Document>("config");

        let mut doc1_cursor = collection.find(doc! {
            "_id": "c2",
        }).unwrap();

        assert!(doc1_cursor.advance().unwrap());

        let doc1 = doc1_cursor.deserialize_current().unwrap();

        assert_eq!(doc1.get("value").unwrap().as_str().unwrap(), "c22");
    });
}

#[test]
fn test_find() {
    vec![
        create_file_and_return_db_with_items("test-find", TEST_SIZE),
        create_memory_and_return_db_with_items(TEST_SIZE),
    ].iter().for_each(|db| {
        let collection = db.collection::<Document>("test");

        let result = collection
            .find(doc! {
                "content": "3",
            })
            .unwrap()
            .collect::<DbResult<Vec<Document>>>()
            .unwrap();

        assert_eq!(result.len(), 1);

        let one = result[0].clone();
        assert_eq!(one.get("content").unwrap().as_str().unwrap(), "3");
    });
}

#[test]
fn test_find_empty_collection() {
    let db = Database::open_memory().unwrap();

    let collection = db.collection::<Document>("test");

    let mut cursor = collection.find(None).unwrap();

    assert!(!cursor.advance().unwrap());
}
