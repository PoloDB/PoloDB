/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use polodb_core::bson::{Document, doc};
use polodb_core::{Database, Collection, Result};
mod common;

use common::{
    prepare_db,
    create_file_and_return_db_with_items,
    create_memory_and_return_db_with_items,
};

static TEST_SIZE: usize = 1000;

#[test]
fn test_create_collection_and_find_all() {
    vec![
        create_file_and_return_db_with_items("test-collection", TEST_SIZE),
        create_memory_and_return_db_with_items(TEST_SIZE),
    ].iter().for_each(|db| {
        let test_collection = db.collection::<Document>("test");
        let cursor = test_collection.find(None).unwrap();

        let all = cursor.collect::<Result<Vec<Document>>>().unwrap();

        let second = test_collection.find_one(doc! {
            "content": "1",
        }).unwrap().unwrap();

        assert_eq!(second.get("content").unwrap().as_str().unwrap(), "1");
        assert!(second.get("content").is_some());

        assert_eq!(TEST_SIZE, all.len())
    });
}

#[test]
fn test_create_collection_and_drop() {
    vec![
        prepare_db("test-create-and-drops").unwrap(),
        Database::open_memory().unwrap(),
    ].iter().for_each(|db| {
        let names = db.list_collection_names().unwrap();
        assert_eq!(names.len(), 0);

        let collection = db.collection::<Document>("test");
        let insert_result = collection.insert_many(&vec![
            doc! {
                "name": "Apple"
            },
            doc! {
                "name": "Banana"
            },
        ]).unwrap();

        assert_eq!(insert_result.inserted_ids.len(), 2);

        let names = db.list_collection_names().unwrap();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], "test");

        let collection = db.collection::<Document>("test");
        collection.drop().unwrap();

        let names = db.list_collection_names().unwrap();
        assert_eq!(names.len(), 0);
    });
}

#[test]
fn test_create_collection_with_number_pkey() {
    vec![
        prepare_db("test-number-pkey").unwrap(),
        Database::open_memory().unwrap()
    ].iter().for_each(|db| {
        let collection = db.collection::<Document>("test");
        let mut data: Vec<Document> = vec![];

        for i in 0..TEST_SIZE {
            let content = i.to_string();
            let new_doc = doc! {
                    "_id": i as i64,
                    "content": content,
                };
            data.push(new_doc);
        }

        collection.insert_many(&data).unwrap();

        let collection: Collection<Document> = db.collection::<Document>("test");

        let count = collection.count_documents().unwrap();
        assert_eq!(TEST_SIZE, count as usize);

        let all = collection
            .find(None)
            .unwrap()
            .collect::<Result<Vec<Document>>>()
            .unwrap();

        assert_eq!(TEST_SIZE, all.len())
    });
}

#[test]
fn test_create_collection_and_find_by_pkey() {
    vec![
        create_file_and_return_db_with_items("test-find-pkey", 10),
        create_memory_and_return_db_with_items(10),
    ].iter().for_each(|db| {
        let collection = db.collection::<Document>("test");

        let all = collection
            .find(None)
            .unwrap()
            .collect::<Result<Vec<Document>>>()
            .unwrap();

        assert_eq!(all.len(), 10);

        let first_key = &all[0].get("_id").unwrap();

        let result = collection
            .find(doc! {
                "_id": first_key.clone(),
            })
            .unwrap()
            .collect::<Result<Vec<Document>>>()
            .unwrap();

        assert_eq!(result.len(), 1);
    });
}
