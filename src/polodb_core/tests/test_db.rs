/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use polodb_core::{Database, Config, DbErr};
use polodb_core::bson::{doc, Document};

mod common;

use common::{
    create_file_and_return_db_with_items,
    mk_db_path,
};
use polodb_core::test_utils::mk_journal_path;

static TEST_SIZE: usize = 1000;

#[test]
fn test_reopen_db() {
    let db_path = mk_db_path("test-reopen");
    let journal_path = mk_journal_path("test-reopen");

    let _ = std::fs::remove_file(db_path.as_path());
    let _ = std::fs::remove_file(journal_path);

    {
        let db = Database::open_file(db_path.as_path().to_str().unwrap()).unwrap();

        let collection = db.collection("books");
        collection.insert_one(doc! {
           "title": "The Three-Body Problem",
           "author": "Liu Cixin",
        }).unwrap();
    }

    {
        let db = Database::open_file(db_path.as_path().to_str().unwrap()).unwrap();
        let collection = db.collection::<Document>("books");
        let mut cursor = collection.find(doc! {}).unwrap();
        assert!(cursor.advance().unwrap());
        let book = cursor.deserialize_current().unwrap();
        assert_eq!(book.get("author").unwrap().as_str().unwrap(), "Liu Cixin");
    }
}

#[test]
fn test_db_occupied() {
    const DB_NAME: &'static str = "test-db-lock";
    let db_path = mk_db_path(DB_NAME);
    let _ = std::fs::remove_file(&db_path);
    {
        let config = Config::default();
        let _db1 = Database::open_file_with_config(db_path.as_path().to_str().unwrap(), config).unwrap();
        let config = Config::default();
        let db2 = Database::open_file_with_config(db_path.as_path().to_str().unwrap(), config);
        match db2 {
            Err(DbErr::DatabaseOccupied) => assert!(true),
            Err(other_error) => {
                println!("{:?}", other_error);
                assert!(false);
            }
            _ => assert!(false),
        }
    }
    let config = Config::default();
    let _db3 = Database::open_file_with_config(db_path.as_path().to_str().unwrap(), config).unwrap();
}

#[test]
fn test_multi_threads() {
    use std::thread;
    use std::sync::Arc;

    let db = {
        let raw = create_file_and_return_db_with_items("test-collection", TEST_SIZE);
        Arc::new(raw)
    };
    let db2 = db.clone();

    let t = thread::spawn(move || {
        let collection = db2.collection("test2");
        collection.insert_one(doc! {
                "content": "Hello"
            }).unwrap();
    });

    t.join().unwrap();

    let collection = db.collection::<Document>("test2");
    let mut cursor = collection.find(doc! {}).unwrap();
    assert!(cursor.advance().unwrap());
    let one = cursor.deserialize_current().unwrap();
    assert_eq!(one.get("content").unwrap().as_str().unwrap(), "Hello");
}

