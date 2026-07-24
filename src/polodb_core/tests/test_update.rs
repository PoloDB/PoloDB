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

use polodb_core::options::UpdateOptions;
use polodb_core::{CollectionT, Database, Result};
use polodb_core::bson::{Document, doc};

mod common;

use common::prepare_db;

#[test]
fn test_update_one() {
    vec![
        prepare_db("test-update-one").unwrap(),
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

    let col = db.collection::<Document>("test");
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
    let cursor = col
        .find(doc! {
            "content": "updated!",
        })
        .run()
        .unwrap();
    let result: Vec<Result<Document>> = cursor.collect();
    assert_eq!(result.len(), 500);
    assert_eq!(result[0].as_ref().unwrap().get("_id").unwrap().as_i32().unwrap(), 500);
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
    let mut cursor = col
        .find(doc! {
            "_id": 0,
        })
        .run()
        .unwrap();
    assert!(cursor.advance().unwrap());
    let result = cursor.deserialize_current().unwrap();
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
    let mut cursor = col.find(doc! {
        "_id": 1,
    }).run().unwrap();
    assert!(cursor.advance().unwrap());
    let result = cursor.deserialize_current().unwrap();
    assert_eq!(result.get("num").unwrap().as_i32().unwrap(), 2);
}

#[test]
fn test_update_min() {
    let db = prepare_db_with_data("test-update-min");
    let col = db.collection::<Document>("test");
    let update_result = col.update_many(doc! {
        "_id": 1,
    }, doc! {
        "$min": {
            "num": 2,
        },
    }).unwrap();
    let result = col.find_one(doc! {
        "_id": 1,
    }).unwrap().unwrap();
    assert_eq!(update_result.modified_count, 0);
    assert_eq!(update_result.matched_count, 1);
    assert_eq!(result.get("num").unwrap().as_i32().unwrap(), 1);
    let update_result = col.update_many(doc! {
        "_id": 1,
    }, doc! {
        "$min": {
            "num": 0,
        },
    }).unwrap();
    let mut cursor = col.find(doc! {
        "_id": 1,
    }).run().unwrap();
    assert!(cursor.advance().unwrap());
    let result = cursor.deserialize_current().unwrap();
    assert_eq!(update_result.modified_count, 1);
    assert_eq!(update_result.matched_count, 1);
    assert_eq!(result.get("num").unwrap().as_i32().unwrap(), 0);

}

#[test]
fn test_update_push() {
    let db = prepare_db("test-update-push").unwrap();
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

#[test]
fn test_upsert() {
    let db = prepare_db("test-upsert").unwrap();
    let col = db.collection::<Document>("test");

    // Attempt to update a non-existent document with upsert
    let update_result = col.update_one_with_options(
        doc! { "_id": 1 },
        doc! { "$set": { "name": "John", "age": 30 } },
        UpdateOptions::builder().upsert(true).build(),
    ).unwrap();

    // Check that the document was inserted
    assert_eq!(update_result.matched_count, 0);
    assert_eq!(update_result.modified_count, 0);
    // assert!(update_result.upserted_id.is_some());

    // Verify the inserted document
    let result = col.find_one(doc! { "name": "John" }).unwrap().unwrap();
    assert_eq!(result.get_str("name").unwrap(), "John");
    assert_eq!(result.get_i32("age").unwrap(), 30);

    // Update the existing document with upsert
    let update_result = col.update_one_with_options(
        doc! { "name": "John" },
        doc! { "$set": { "age": 31 }, "$push": { "hobbies": "reading" } },
        UpdateOptions::builder().upsert(true).build(),
    ).unwrap();

    // Check that the document was updated
    assert_eq!(update_result.matched_count, 1);
    assert_eq!(update_result.modified_count, 1);
    // assert!(update_result.upserted_id.is_none());

    // // Verify the updated document
    let result = col.find_one(doc! { "name": "John" }).unwrap().unwrap();
    assert_eq!(result.get_str("name").unwrap(), "John");
    assert_eq!(result.get_i32("age").unwrap(), 31);
    // let hobbies = result.get_array("hobbies").unwrap();
    // assert_eq!(hobbies.len(), 1);
    // assert_eq!(hobbies[0].as_str().unwrap(), "reading");
}

#[test]
fn test_upsert_does_not_insert_when_update_is_noop() {
    let db = prepare_db("test-upsert-noop").unwrap();
    let col = db.collection::<Document>("test");
    let original = doc! {
        "_id": 1,
        "name": "John",
        "age": 30,
    };
    col.insert_one(original.clone()).unwrap();

    let update_result = col
        .update_one_with_options(
            doc! { "name": "John" },
            doc! { "$set": {} },
            UpdateOptions::builder().upsert(true).build(),
        )
        .unwrap();

    assert_eq!(update_result.matched_count, 1);
    assert_eq!(update_result.modified_count, 0);
    assert_eq!(col.count_documents().unwrap(), 1);
    assert_eq!(col.find_one(doc! { "_id": 1 }).unwrap(), Some(original));
}

#[test]
fn test_update_dotted_paths() {
    let db = prepare_db("test-update-dotted-paths").unwrap();
    let col = db.collection::<Document>("test");
    col.insert_one(doc! {
        "_id": 1,
        "profile": {
            "name": "Vincent",
            "stats": { "visits": 2 },
            "address": { "city": "London", "zip": "E1" },
            "tags": ["rust"],
        },
    })
    .unwrap();

    let result = col
        .update_one(
            doc! { "_id": 1 },
            doc! {
                "$set": {
                    "profile.name": "Steve",
                    "profile.preferences.theme": "dark",
                },
                "$unset": { "profile.address.city": "" },
                "$inc": { "profile.stats.visits": 3 },
                "$push": { "profile.tags": "database" },
            },
        )
        .unwrap();
    assert_eq!(result.modified_count, 1);

    let actual = col.find_one(doc! { "_id": 1 }).unwrap().unwrap();
    let profile = actual.get_document("profile").unwrap();
    assert_eq!(profile.get_str("name").unwrap(), "Steve");
    assert_eq!(
        profile
            .get_document("preferences")
            .unwrap()
            .get_str("theme")
            .unwrap(),
        "dark"
    );
    assert_eq!(
        profile
            .get_document("stats")
            .unwrap()
            .get_i32("visits")
            .unwrap(),
        5
    );
    let address = profile.get_document("address").unwrap();
    assert!(!address.contains_key("city"));
    assert_eq!(address.get_str("zip").unwrap(), "E1");
    assert_eq!(
        profile.get_array("tags").unwrap(),
        &vec!["rust".into(), "database".into()]
    );
}

#[test]
fn test_dotted_update_rejects_non_document_intermediate_and_rolls_back() {
    let db = prepare_db("test-update-dotted-non-document").unwrap();
    let col = db.collection::<Document>("test");
    let original = doc! {
        "_id": 1,
        "status": "original",
        "profile": "not a document",
    };
    col.insert_one(original.clone()).unwrap();

    let result = col.update_one(
        doc! { "_id": 1 },
        doc! { "$set": {
            "status": "changed",
            "profile.name": "Steve",
        } },
    );
    assert!(result.is_err());
    assert_eq!(col.find_one(doc! { "_id": 1 }).unwrap().unwrap(), original);
}

#[test]
fn test_dotted_update_rejects_primary_key_descendants_and_conflicts() {
    let db = prepare_db("test-update-dotted-invalid-paths").unwrap();
    let col = db.collection::<Document>("test");
    col.insert_one(doc! { "_id": 1, "profile": {} }).unwrap();

    assert!(col
        .update_one(
            doc! { "_id": 1 },
            doc! { "$set": { "_id.value": 2 } },
        )
        .is_err());
    assert!(col
        .update_one(
            doc! { "_id": 1 },
            doc! { "$set": { "profile": {}, "profile.name": "Steve" } },
        )
        .is_err());
}

#[test]
fn test_remaining_update_operators_support_dotted_paths() {
    let db = prepare_db("test-update-remaining-dotted-paths").unwrap();
    let col = db.collection::<Document>("test");
    col.insert_one(doc! {
        "_id": 1,
        "profile": {
            "score": 2,
            "minimum": 5,
            "maximum": 5,
            "queue": [1, 2],
            "old_name": "Vincent",
        },
    })
    .unwrap();

    col.update_one(
        doc! { "_id": 1 },
        doc! {
            "$mul": { "profile.score": 3 },
            "$min": { "profile.minimum": 2 },
            "$max": { "profile.maximum": 8 },
            "$pop": { "profile.queue": 1 },
            "$rename": { "profile.old_name": "archive.name" },
        },
    )
    .unwrap();

    let actual = col.find_one(doc! { "_id": 1 }).unwrap().unwrap();
    let profile = actual.get_document("profile").unwrap();
    assert_eq!(profile.get_i32("score").unwrap(), 6);
    assert_eq!(profile.get_i32("minimum").unwrap(), 2);
    assert_eq!(profile.get_i32("maximum").unwrap(), 8);
    assert_eq!(profile.get_array("queue").unwrap(), &vec![1.into()]);
    assert!(!profile.contains_key("old_name"));
    assert_eq!(
        actual
            .get_document("archive")
            .unwrap()
            .get_str("name")
            .unwrap(),
        "Vincent"
    );
}
