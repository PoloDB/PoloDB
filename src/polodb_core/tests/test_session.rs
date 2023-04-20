/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::fs::File;
use polodb_core::{Database, DbErr, DbResult, LsmKv};
use polodb_core::bson::{Document, doc};
use csv::Reader;

mod common;

use common::prepare_db;
use polodb_core::test_utils::mk_journal_path;
use crate::common::mk_db_path;

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

#[test]
fn test_page_recycle() {
    let db_name = "test_page_recycle";
    let db_path = mk_db_path(db_name);
    let journal_path = mk_journal_path(db_name);

    let _ = std::fs::remove_file(db_path.as_path());
    let _ = std::fs::remove_file(journal_path);

    let dir = env!("CARGO_MANIFEST_DIR");
    let dataset_path = dir.to_string() + "/tests/dataset/CrimeDataFrom2020.csv";
    let dataset_file = File::open(dataset_path).unwrap();

    let kv = LsmKv::open_file(&db_path).unwrap();
    let metrics = kv.metrics();
    metrics.enable();

    kv.put("a", "ace").unwrap();

    let mut rdr = Reader::from_reader(&dataset_file);
    let mut counter: usize = 0;

    let mut buffer = vec![];

    for result in rdr.records() {
        let record = result.unwrap();
        let record_str = record.get(0).unwrap().to_string();
        buffer.push(record_str);

        counter += 1;
        if counter > 7500 {
            break;
        }
    }

    for (index, str) in buffer.as_slice()[0..1500].iter().enumerate() {
        let index_be: [u8; 8] = index.to_be_bytes();
        kv.put(index_be, str).unwrap();
    }

    // sync once
    assert_eq!(metrics.sync_count(), 1);

    for (index, str) in buffer.as_slice()[0..1500].iter().enumerate() {
        let index_be: [u8; 8] = index.to_be_bytes();
        let result_opt = kv.get_string(index_be).unwrap();
        let result = result_opt.expect(format!("index: {}", index).as_str());
        assert_eq!(result.as_str(), str);
    }

    assert_eq!(metrics.clone_snapshot_count(), 0);

    let spare_session = kv.new_session();

    for (index, str) in buffer.as_slice()[1500..].iter().enumerate() {
        let index = index + 1500;
        let index_be: [u8; 8] = index.to_be_bytes();
        kv.put(index_be, str).unwrap();
    }

    assert_eq!(metrics.clone_snapshot_count(), 1);
    assert_eq!(metrics.minor_compact(), 1);

    for (index, str) in buffer.as_slice()[0..1500].iter().enumerate() {
        let index_be: [u8; 8] = index.to_be_bytes();
        let result_opt = kv.get_string_with_session(index_be, &spare_session).unwrap();
        let result = result_opt.expect(format!("index: {}", index).as_str());
        assert_eq!(result.as_str(), str);
    }

    for (index, _) in buffer.as_slice()[1500..].iter().enumerate() {
        let index = index + 1500;
        let index_be: [u8; 8] = index.to_be_bytes();
        let result_opt = kv.get_string_with_session(index_be, &spare_session).unwrap();
        assert!(result_opt.is_none());
    }
}
