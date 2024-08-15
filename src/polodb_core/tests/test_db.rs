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

use polodb_core::Database;
use polodb_core::bson::{doc, Document};
use polodb_core::CollectionT;

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
        let db = Database::open_path(db_path.as_path().to_str().unwrap()).unwrap();

        let collection = db.collection("books");
        collection.insert_one(doc! {
           "title": "The Three-Body Problem",
           "author": "Liu Cixin",
        }).unwrap();
    }

    {
        let db = Database::open_path(db_path.as_path().to_str().unwrap()).unwrap();
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

