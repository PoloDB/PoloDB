/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use polodb_core::Database;
use polodb_core::bson::{doc, Document};

mod common;

use common::{
    create_file_and_return_db_with_items,
    mk_db_path,
};

static TEST_SIZE: usize = 1000;

#[test]
fn test_reopen_db() {
    let db_path = mk_db_path("test-reopen");

    let _ = std::fs::remove_dir_all(db_path.as_path());

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
        let book = collection.find_one(None).unwrap().unwrap();
        assert_eq!(book.get("author").unwrap().as_str().unwrap(), "Liu Cixin");
    }
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
    let one = collection.find_one(None).unwrap().unwrap();
    assert_eq!(one.get("content").unwrap().as_str().unwrap(), "Hello");
}

