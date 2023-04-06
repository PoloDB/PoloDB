/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use polodb_core::{Collection, Database};
use polodb_core::bson::{Document, doc};

mod common;

use common::prepare_db;

#[test]
fn test_update_one() {
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

        let result = collection.update_one(doc! {
                "name": "Vincent",
            }, doc! {
                "$set": {
                    "name": "Steve",
                }
            }).unwrap();

        assert_eq!(result.modified_count, 1);
    });
}

fn prepare_db_with_data(db_name: &str) -> Database {
    let db = prepare_db(db_name).unwrap();

    let mut arr = vec![];

    for i in 0..1000 {
        arr.push(doc! {
            "_id": i,
            "num": i,
            "content": i.to_string(),
        });
    }

    let col: Collection<Document> = db.collection("test");
    col.insert_many(arr).unwrap();

    db
}

#[test]
fn test_update_gte_set() {
    let db = prepare_db_with_data("test-update-many");
    let col = db.collection::<Document>("test");
    let update_result = col.update_many(doc! {
        "_id": {
            "$gte": 500,
        },
    }, doc! {
        "$set": {
            "content": "updated!",
        },
    }).unwrap();
    assert_eq!(update_result.modified_count, 500);
    let result = col.find_many(doc! {
        "content": "updated!",
    }).unwrap();
    assert_eq!(result.len(), 500);
    assert_eq!(result[0].get("_id").unwrap().as_i32().unwrap(), 500);
}

#[test]
fn test_throw_error_while_updating_primary_key() {
    let db = prepare_db_with_data("test-update-pkey");
    let col = db.collection::<Document>("test");
    let result = col.update_many(doc! {
        "_id": 0,
    }, doc! {
        "$inc": {
            "_id": 100,
        },
    });
    assert!(result.is_err());
}

#[test]
fn test_update_inc() {
    let db = prepare_db_with_data("test-update-inc");
    let col = db.collection::<Document>("test");
    col.update_many(doc! {
        "_id": 0,
    }, doc! {
        "$inc": {
            "num": 100,
        },
    }).unwrap();
    let result = col.find_one(doc! {
        "_id": 0,
    }).unwrap().unwrap();
    assert_eq!(result.get("num").unwrap().as_i32().unwrap(), 100);
}

#[test]
fn test_update_rename() {
    let db = prepare_db_with_data("test-update-rename");
    let col = db.collection::<Document>("test");
    col.update_many(doc! {
        "_id": 0,
    }, doc! {
        "$rename": {
            "num": "num2",
        },
    }).unwrap();
    let result = col.find_one(doc! {
        "_id": 0,
    }).unwrap().unwrap();
    println!("result: {}", result);
    assert_eq!(result.get("_id").unwrap().as_i32().unwrap(), 0);
    assert!(result.get("num").is_none());
    assert_eq!(result.get("num2").unwrap().as_i32().unwrap(), 0);
}

#[test]
fn test_update_unset() {
    let db = prepare_db_with_data("test-update-unset");
    let col = db.collection::<Document>("test");
    col.update_many(doc! {
        "_id": 0,
    }, doc! {
        "$unset": {
            "num": "",
        },
    }).unwrap();
    let result = col.find_one(doc! {
        "_id": 0,
    }).unwrap().unwrap();
    assert!(result.get("num").is_none());
}

#[test]
fn test_update_max() {
    let db = prepare_db_with_data("test-update-max");
    let col = db.collection::<Document>("test");
    col.update_many(doc! {
        "_id": 1,
    }, doc! {
        "$max": {
            "num": 0,
        },
    }).unwrap();
    let result = col.find_one(doc! {
        "_id": 1,
    }).unwrap().unwrap();
    assert_eq!(result.get("num").unwrap().as_i32().unwrap(), 1);
    col.update_many(doc! {
        "_id": 1,
    }, doc! {
        "$max": {
            "num": 2,
        },
    }).unwrap();
    let result = col.find_one(doc! {
        "_id": 1,
    }).unwrap().unwrap();
    assert_eq!(result.get("num").unwrap().as_i32().unwrap(), 2);
}

#[test]
fn test_update_push() {
    let db = prepare_db("test-update-max").unwrap();
    let col = db.collection::<Document>("test");
    let insert_doc = doc! {
        "_id": 0,
        "content": [1, 2, 3],
    };
    col.insert_one(insert_doc).unwrap();
    let update_result = col.update_many(doc! {
        "_id": 0,
    }, doc! {
        "$push": {
            "content": 4,
        },
    }).unwrap();
    assert_eq!(update_result.modified_count, 1);
    let result = col.find_one(doc! {
        "_id": 0,
    }).unwrap().unwrap();
    let content = result.get_array("content").unwrap();
    assert_eq!(content.len(), 4);
}
