/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use polodb_core::Result;
use polodb_core::bson::{Document, doc};

mod common;

use common::prepare_db;

#[test]
fn test_transaction_commit() {
    vec![
        prepare_db("test-transaction-commit").unwrap(),
    ].iter().for_each(|db| {
        let txn = db.start_transaction().unwrap();

        let collection = txn.collection::<Document>("test");

        for i in 0..10 {
            let content = i.to_string();
            let mut new_doc = doc! {
                "_id": i,
                "content": content,
            };
            collection.insert_one(&mut new_doc).unwrap();
        }

        txn.commit().unwrap();

        let collection = db.collection::<Document>("test");

        let doc = collection
            .find(None)
            .unwrap()
            .collect::<Result<Vec<Document>>>()
            .unwrap();
        assert_eq!(doc.len(), 10);
    });
}

#[test]
fn test_commit_after_commit() {
    let db = prepare_db("test-commit-2").unwrap();

    let txn = db.start_transaction().unwrap();
    let collection = txn.collection::<Document>("test");

    for i in 0..1000 {
        let content = i.to_string();
        let new_doc = doc! {
            "_id": i,
            "content": content,
        };
        collection.insert_one(new_doc).unwrap();
    }
    txn.commit().unwrap();

    let txn = db.start_transaction().unwrap();
    let collection2 = txn.collection::<Document>("test-2");
    for i in 0..10{
        let content = i.to_string();
        let new_doc = doc! {
            "_id": i,
            "content": content,
        };
        collection2.insert_one(new_doc).expect(&*format!("insert failed: {}", i));
    }
    txn.commit().unwrap()
}

#[test]
fn test_rollback() {
    vec![
        prepare_db("test-collection").unwrap(),
    ].iter().for_each(|db| {


        let txn = db.start_transaction().unwrap();
        let collection = txn.collection::<Document>("test");
        assert_eq!(collection.count_documents().unwrap(), 0);

        for i in 0..10 {
            let content = i.to_string();
            let new_doc = doc! {
                "_id": i,
                "content": content,
            };
            collection.insert_one(new_doc).unwrap();
        }
        assert_eq!(collection.count_documents().unwrap(), 10);

        txn.rollback().unwrap();

        let collection = db.collection::<Document>("test");
        assert_eq!(collection.count_documents().unwrap(), 0);
    });
}
