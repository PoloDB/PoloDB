/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use polodb_core::{Database, DbErr, DbResult};
use polodb_core::bson::{Document, doc};

mod common;

use common::prepare_db;

#[test]
fn test_transaction_commit() {
    vec![
        prepare_db("test-transaction-commit").unwrap(),
        Database::open_memory().unwrap(),
    ].iter().for_each(|db| {
        let mut session = db.start_session().unwrap();
        session.start_transaction(None).unwrap();

        let collection = db.collection::<Document>("test");

        for i in 0..10 {
            let content = i.to_string();
            let mut new_doc = doc! {
                "_id": i,
                "content": content,
            };
            collection.insert_one_with_session(&mut new_doc, &mut session).unwrap();
        }

        session.commit_transaction().unwrap();

        let doc = collection
            .find(None)
            .unwrap()
            .collect::<DbResult<Vec<Document>>>()
            .unwrap();
        assert_eq!(doc.len(), 10);
    });
}

#[test]
fn test_session_outdated() {
    let db = prepare_db("test-session-outdate").unwrap();
    let col = db.collection::<Document>("test");

    let mut session = db.start_session().unwrap();
    session.start_transaction(None).unwrap();

    col.insert_one(doc! {
        "name": "Vincent",
    }).unwrap();

    col.insert_one_with_session(doc! {
        "name": "Vincent",
    }, &mut session).unwrap();

    let result = session.commit_transaction();
    assert!(match result {
        Err(DbErr::SessionOutdated) => true,
        _ => false,
    })
}


#[test]
fn test_commit_after_commit() {
    let db = prepare_db("test-commit-2").unwrap();

    let mut session = db.start_session().unwrap();
    session.start_transaction(None).unwrap();
    let collection = db.collection::<Document>("test");

    for i in 0..1000 {
        let content = i.to_string();
        let new_doc = doc! {
            "_id": i,
            "content": content,
        };
        collection.insert_one_with_session(new_doc, &mut session).unwrap();
    }
    session.commit_transaction().unwrap();

    session.start_transaction(None).unwrap();
    let collection2 = db.collection::<Document>("test-2");
    for i in 0..10{
        let content = i.to_string();
        let new_doc = doc! {
            "_id": i,
            "content": content,
        };
        collection2.insert_one_with_session(new_doc, &mut session).expect(&*format!("insert failed: {}", i));
    }
    session.commit_transaction().unwrap();
}

#[test]
fn test_rollback() {
    vec![
        prepare_db("test-collection").unwrap(),
        Database::open_memory().unwrap(),
    ].iter().for_each(|db| {
        let collection = db.collection::<Document>("test");

        assert_eq!(collection.count_documents().unwrap(), 0);

        let mut session = db.start_session().unwrap();
        session.start_transaction(None).unwrap();

        for i in 0..10 {
            let content = i.to_string();
            let new_doc = doc! {
                "_id": i,
                "content": content,
            };
            collection.insert_one_with_session(new_doc, &mut session).unwrap();
        }
        assert_eq!(collection.count_documents_with_session(&mut session).unwrap(), 10);

        session.abort_transaction().unwrap();

        assert_eq!(collection.count_documents_with_session(&mut session).unwrap(), 0);
    });
}
