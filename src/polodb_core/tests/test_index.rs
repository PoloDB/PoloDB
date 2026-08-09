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

use polodb_core::{Collection, CollectionT, Database, IndexModel, IndexOptions, Result};
use polodb_core::options::UpdateOptions;
use bson::{doc, Document};
use crate::common::prepare_db;

mod common;

fn create_unique_name_index(col: &Collection<Document>) {
    col.create_index(IndexModel {
        keys: doc! {
            "name": 1,
        },
        options: Some(IndexOptions {
            unique: Some(true),
            ..Default::default()
        }),
    }).unwrap();
}

fn create_tags_index(col: &Collection<Document>, unique: bool) {
    col.create_index(IndexModel {
        keys: doc! {
            "tags": 1,
        },
        options: unique.then_some(IndexOptions {
            unique: Some(true),
            ..Default::default()
        }),
    }).unwrap();
}

#[test]
fn test_create_multi_keys_index() {
    let db = prepare_db("test-create-multi-keys-index").unwrap();
    let col = db.collection::<Document>("teacher");
    let result = col.create_index(IndexModel {
        keys: doc! {
            "age": 1,
            "name": 1,
        },
        options: None,
    });
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("only support single field indexes currently"));
}

#[test]
fn test_create_reverse_order_index() {
    let db = prepare_db("test-create-reverse-order-index").unwrap();
    let col = db.collection::<Document>("teacher");
    let result = col.create_index(IndexModel {
        keys: doc! {
            "age": -1,
        },
        options: None,
    });
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("only support ascending order index currently"));
}

#[test]
fn test_create_index() {
    vec![
        prepare_db("test-create-index").unwrap(),
    ].iter().for_each(|db| {
        let col = db.collection("teacher");

        col.create_index(IndexModel {
            keys: doc! {
                "age": 1,
            },
            options: None,
        }).unwrap();

        col.insert_one(doc! {
            "name": "David",
            "age": 33,
        }).unwrap();
    });
}

#[test]
fn test_create_index_with_data() {
    vec![
        prepare_db("test-create-index-with-data").unwrap(),
    ].iter().for_each(|db| {
        let metrics = db.metrics();
        metrics.enable();

        let col = db.collection::<Document>("teacher");

        col.insert_one(doc! {
            "name": "David",
            "age": 33,
        }).unwrap();

        col.create_index(IndexModel {
            keys: doc! {
                "name": 1,
            },
            options: None,
        }).unwrap();

        let doc = col.find_one(doc! {
            "name": "David"
        }).unwrap().unwrap();

        assert_eq!(doc.get_str("name").unwrap(), "David");

        assert_eq!(metrics.find_by_index_count(), 1);
    });
}

#[test]
fn test_find_by_index() {
    vec![
        prepare_db("test-find-by-index").unwrap(),
    ].iter().for_each(|db| {
        let metrics = db.metrics();
        metrics.enable();

        let col = db.collection::<Document>("teacher");

        col.create_index(IndexModel {
            keys: doc! {
                "name": 1,
            },
            options: None,
        }).unwrap();

        col.insert_one(doc! {
            "name": "David",
            "age": 33,
        }).unwrap();

        let doc = col.find_one(doc! {
            "name": "David"
        }).unwrap().unwrap();

        assert_eq!(doc.get_str("name").unwrap(), "David");

        assert_eq!(metrics.find_by_index_count(), 1);
    });
}

#[test]
fn test_index_order() {
    vec![
        prepare_db("test-index-order").unwrap(),
    ].iter().for_each(|db| {
        let metrics = db.metrics();
        metrics.enable();

        let col = db.collection::<Document>("teacher");

        col.create_index(IndexModel {
            keys: doc! {
                "age": 1,
            },
            options: None,
        }).unwrap();

        col.insert_many(vec![
            doc! {
                "name": "David",
                "age": 23,
            },
            doc! {
                "name": "John",
            },
            doc! {
                "name": "Dick",
                "age": 23,
            }
        ]).unwrap();

        let people23 = col
            .find(doc! {
                "age": 23
            })
            .run()
            .unwrap()
            .collect::<Result<Vec<Document>>>()
            .unwrap();

        assert_eq!(people23.len(), 2);
    });
}

#[test]
fn test_create_unique_index() {
    vec![
        prepare_db("test-create-unique-index").unwrap(),
    ].iter().for_each(|db| {
        let col = db.collection("teacher");

        col.create_index(IndexModel {
            keys: doc! {
                "name": 1,
            },
            options: Some(IndexOptions{
                unique: Some(true),
                ..Default::default()
            }),
        }).unwrap();

        col.insert_one(doc! {
            "name": "David",
            "age": 33,
        }).unwrap();

        let result = col.insert_one(doc! {
            "name": "David",
            "age": 33,
        });

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("duplicate key error"));
    });
}

#[test]
fn test_update_non_index_field_with_unique_index() {
    let db = prepare_db("test-update-non-index-field-with-unique-index").unwrap();
    let col = db.collection::<Document>("teacher");

    create_unique_name_index(&col);

    col.insert_one(doc! {
        "name": "David",
        "age": 33,
    }).unwrap();

    let result = col.update_one(doc! {
        "name": "David",
    }, doc! {
        "$set": {
            "age": 34,
        },
    }).unwrap();

    assert_eq!(result.matched_count, 1);
    assert_eq!(result.modified_count, 1);
    assert_eq!(
        col.find_one(doc! {
            "name": "David",
        }).unwrap().unwrap().get_i32("age").unwrap(),
        34,
    );
}

#[test]
fn test_upsert_updates_non_index_field_with_unique_index() {
    let db = prepare_db("test-upsert-update-with-unique-index").unwrap();
    let col = db.collection::<Document>("teacher");

    create_unique_name_index(&col);

    col.insert_one(doc! {
        "name": "David",
        "age": 33,
    }).unwrap();

    let result = col.update_one_with_options(
        doc! {
            "name": "David",
        },
        doc! {
            "$set": {
                "age": 34,
            },
        },
        UpdateOptions::builder().upsert(true).build(),
    ).unwrap();

    assert_eq!(result.matched_count, 1);
    assert_eq!(result.modified_count, 1);
    assert_eq!(col.count_documents().unwrap(), 1);
    assert_eq!(
        col.find_one(doc! {
            "name": "David",
        }).unwrap().unwrap().get_i32("age").unwrap(),
        34,
    );
}

#[test]
fn test_update_unique_index_value() {
    let db = prepare_db("test-update-unique-index-value").unwrap();
    let metrics = db.metrics();

    let col = db.collection::<Document>("teacher");

    create_unique_name_index(&col);

    col.insert_one(doc! {
        "name": "David",
        "age": 33,
    }).unwrap();

    let result = col.update_one(doc! {
        "name": "David",
    }, doc! {
        "$set": {
            "name": "Daniel",
        },
    }).unwrap();

    assert_eq!(result.matched_count, 1);
    assert_eq!(result.modified_count, 1);

    metrics.enable();
    assert_eq!(
        col.find_one(doc! {
            "name": "Daniel",
        }).unwrap().unwrap().get_i32("age").unwrap(),
        33,
    );
    assert_eq!(metrics.find_by_index_count(), 1);
    assert!(col.find_one(doc! {
        "name": "David",
    }).unwrap().is_none());
}

#[test]
fn test_update_unique_index_to_duplicate_rolls_back() {
    let db = prepare_db("test-update-unique-index-to-duplicate").unwrap();
    let col = db.collection::<Document>("teacher");

    create_unique_name_index(&col);

    col.insert_many(vec![
        doc! {
            "name": "David",
            "age": 33,
        },
        doc! {
            "name": "Harry",
            "age": 32,
        },
    ]).unwrap();

    let result = col.update_one(doc! {
        "name": "David",
    }, doc! {
        "$set": {
            "name": "Harry",
        },
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("duplicate key error"));
    assert_eq!(col.count_documents().unwrap(), 2);
    assert_eq!(
        col.find_one(doc! {
            "name": "David",
        }).unwrap().unwrap().get_i32("age").unwrap(),
        33,
    );
    assert_eq!(
        col.find_one(doc! {
            "name": "Harry",
        }).unwrap().unwrap().get_i32("age").unwrap(),
        32,
    );
}

#[test]
fn test_delete_with_unique_index() {
    let db = prepare_db("test-delete-with-unique-index").unwrap();
    let col = db.collection::<Document>("teacher");

    create_unique_name_index(&col);

    col.insert_one(doc! {
        "name": "David",
        "age": 33,
    }).unwrap();

    let result = col.delete_one(doc! {
        "name": "David",
    }).unwrap();

    assert_eq!(result.deleted_count, 1);
    assert_eq!(col.count_documents().unwrap(), 0);
    assert!(col.find_one(doc! {
        "name": "David",
    }).unwrap().is_none());
}

#[test]
fn test_update_with_index() {
    vec![
        prepare_db("test-update-with-index").unwrap(),
    ].iter().for_each(|db| {
        let metrics = db.metrics();
        metrics.enable();

        let col = db.collection("teacher");

        col.create_index(IndexModel {
            keys: doc! {
                "name": 1,
            },
            options: None,
        }).unwrap();

        col.insert_one(doc! {
            "name": "David",
            "age": 33,
        }).unwrap();

        col.update_many(doc! {
            "name": "David",
        }, doc! {
            "$set": {
                "name": "David Chan",
            },
        }).unwrap();

        let doc = col.find_one(doc! {
            "name": "David Chan"
        }).unwrap().unwrap();

        assert_eq!(doc.get_i32("age").unwrap(), 33);

        assert_eq!(metrics.find_by_index_count(), 1);
    });
}

#[test]
fn test_delete_with_index() {
    vec![
        prepare_db("test-delete-with-index").unwrap(),
    ].iter().for_each(|db| {
        let metrics = db.metrics();
        metrics.enable();

        let col = db.collection::<Document>("teacher");

        col.create_index(IndexModel {
            keys: doc! {
                "age": 1,
            },
            options: None,
        }).unwrap();

        col.insert_one(doc! {
            "name": "David",
            "age": 33,
        }).unwrap();

        let result = col.delete_many(doc! {
            "age": 33,
        }).unwrap();

        assert_eq!(result.deleted_count, 1);

        let count = col.count_documents().unwrap();
        assert_eq!(count, 0);

        let result = col.find_one(doc! {
            "age": 33
        }).unwrap();
        assert_eq!(result, None);

        assert!(col.find_one(doc! {
            "age": 33
        }).unwrap().is_none());
    });
}

#[test]
fn test_drop_index() {
    vec![
        prepare_db("test-drop-index").unwrap(),
    ].iter().for_each(|db| {
        let metrics = db.metrics();
        metrics.enable();

        let col = db.collection::<Document>("teacher");

        col.create_index(IndexModel {
            keys: doc! {
                "age": 1,
            },
            options: None,
        }).unwrap();

        col.insert_one(doc! {
            "name": "David",
            "age": 33,
        }).unwrap();

        col.drop_index("age_1").unwrap();

        {
            let doc = col.find_one(doc! {
                "age": 33,
            }).unwrap().unwrap();

            assert_eq!(doc.get_str("name").unwrap(), "David");
        }

        assert_eq!(metrics.find_by_index_count(), 0);
    });
}

#[test]
fn test_issue_171() {
    let db = prepare_db("test-issue-171").unwrap();

    let col = db.collection::<Document>("teacher");

    col.create_index(IndexModel {
        keys: doc! {
                "name": 1,
            },
        options: None,
    }).unwrap();

    col.insert_one(doc! {
        "name": "David",
        "age": 33,
    }).unwrap();

    col.insert_one(doc! {
        "name": "Harry",
        "age": 32,
    }).unwrap();

    let doc = col.find_one(doc! {
        "name": "David"
    }).unwrap().unwrap();

    assert_eq!(doc.get_str("name").unwrap(), "David");

    let docs:Vec<Document> = col.find(doc! {
        "name": "David"
    }).run().unwrap().into_iter().map(|cc| cc.unwrap()).collect();

    assert_eq!(docs.len(), 1);

    assert_eq!(docs[0].get_str("name").unwrap(), "David");
}

#[test]
fn test_issue_147_dotted_scalar_array_index() {
    let db = prepare_db("test-issue-147-dotted-scalar-array-index").unwrap();
    let metrics = db.metrics();
    let col = db.collection::<Document>("music");

    col.create_index(IndexModel {
        keys: doc! {
            "metadata.tags": 1,
        },
        options: None,
    }).unwrap();

    col.insert_many(vec![
        doc! {
            "_id": 1,
            "metadata": {
                "tags": ["ambient", "electronic"],
            },
        },
        doc! {
            "_id": 2,
            "metadata": {
                "tags": ["jazz"],
            },
        },
    ]).unwrap();

    metrics.enable();
    let matches = col.find(doc! {
        "metadata.tags": "ambient",
    })
        .run()
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();

    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].get_i32("_id").unwrap(), 1);
    assert_eq!(metrics.find_by_index_count(), 1);
}

#[test]
fn test_create_multikey_index_with_existing_data() {
    let db = prepare_db("test-create-multikey-index-with-existing-data").unwrap();
    let metrics = db.metrics();
    let col = db.collection::<Document>("music");

    col.insert_one(doc! {
        "_id": 1,
        "tags": ["rock", "live"],
    }).unwrap();
    create_tags_index(&col, false);

    metrics.enable();
    assert!(col.find_one(doc! {
        "tags": "live",
    }).unwrap().is_some());
    assert_eq!(metrics.find_by_index_count(), 1);
}

#[test]
fn test_multikey_index_persists_after_reopen() {
    let db_name = "test-multikey-index-persists-after-reopen";
    let db_path = common::mk_db_path(db_name);
    let db = prepare_db(db_name).unwrap();
    let col = db.collection::<Document>("music");

    create_tags_index(&col, false);
    col.insert_one(doc! {
        "_id": 1,
        "tags": ["rock", "live"],
    }).unwrap();

    drop(col);
    drop(db);

    let db = Database::open_path(db_path.to_str().unwrap()).unwrap();
    let metrics = db.metrics();
    let col = db.collection::<Document>("music");
    metrics.enable();

    assert!(col.find_one(doc! {
        "tags": "live",
    }).unwrap().is_some());
    assert_eq!(metrics.find_by_index_count(), 1);
}

#[test]
fn test_unique_multikey_deduplicates_values_within_document() {
    let db = prepare_db("test-unique-multikey-deduplicates-values").unwrap();
    let col = db.collection::<Document>("music");
    create_tags_index(&col, true);

    col.insert_one(doc! {
        "_id": 1,
        "tags": ["rock", "rock", "live"],
    }).unwrap();
    col.insert_one(doc! {
        "_id": 2,
        "tags": ["jazz"],
    }).unwrap();

    assert_eq!(col.count_documents().unwrap(), 2);
}

#[test]
fn test_unique_multikey_conflict_rolls_back_all_values() {
    let db = prepare_db("test-unique-multikey-conflict-rolls-back").unwrap();
    let col = db.collection::<Document>("music");
    create_tags_index(&col, true);

    col.insert_one(doc! {
        "_id": 1,
        "tags": ["rock", "live"],
    }).unwrap();

    let result = col.insert_one(doc! {
        "_id": 2,
        "tags": ["jazz", "rock"],
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("duplicate key error"));
    assert_eq!(col.count_documents().unwrap(), 1);
    assert!(col.find_one(doc! {
        "tags": "jazz",
    }).unwrap().is_none());
    assert_eq!(
        col.find_one(doc! {
            "tags": "rock",
        }).unwrap().unwrap().get_i32("_id").unwrap(),
        1,
    );
}

#[test]
fn test_create_unique_multikey_index_conflict_rolls_back() {
    let db = prepare_db("test-create-unique-multikey-conflict-rolls-back").unwrap();
    let metrics = db.metrics();
    let col = db.collection::<Document>("music");

    col.insert_many(vec![
        doc! {
            "_id": 1,
            "tags": ["rock", "live"],
        },
        doc! {
            "_id": 2,
            "tags": ["jazz", "rock"],
        },
    ]).unwrap();

    let result = col.create_index(IndexModel {
        keys: doc! {
            "tags": 1,
        },
        options: Some(IndexOptions {
            unique: Some(true),
            ..Default::default()
        }),
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("duplicate key error"));

    metrics.enable();
    let matches = col.find(doc! {
        "tags": "rock",
    })
        .run()
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(matches.len(), 2);
    assert_eq!(metrics.find_by_index_count(), 0);
}

#[test]
fn test_multikey_update_and_delete_maintain_all_values() {
    let db = prepare_db("test-multikey-update-and-delete").unwrap();
    let col = db.collection::<Document>("music");
    create_tags_index(&col, false);

    col.insert_one(doc! {
        "_id": 1,
        "tags": ["rock", "live"],
    }).unwrap();

    col.update_one(doc! {
        "_id": 1,
    }, doc! {
        "$set": {
            "tags": ["jazz"],
        },
    }).unwrap();

    assert!(col.find_one(doc! {
        "tags": "rock",
    }).unwrap().is_none());
    assert!(col.find_one(doc! {
        "tags": "live",
    }).unwrap().is_none());
    assert!(col.find_one(doc! {
        "tags": "jazz",
    }).unwrap().is_some());

    col.delete_one(doc! {
        "_id": 1,
    }).unwrap();
    assert!(col.find_one(doc! {
        "tags": "jazz",
    }).unwrap().is_none());
}

#[test]
fn test_unique_multikey_update_conflict_rolls_back() {
    let db = prepare_db("test-unique-multikey-update-conflict").unwrap();
    let col = db.collection::<Document>("music");
    create_tags_index(&col, true);

    col.insert_many(vec![
        doc! {
            "_id": 1,
            "tags": ["rock", "live"],
        },
        doc! {
            "_id": 2,
            "tags": ["jazz", "blues"],
        },
    ]).unwrap();

    let result = col.update_one(doc! {
        "_id": 2,
    }, doc! {
        "$set": {
            "tags": ["jazz", "rock", "new"],
        },
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("duplicate key error"));
    assert_eq!(
        col.find_one(doc! {
            "tags": "jazz",
        }).unwrap().unwrap().get_i32("_id").unwrap(),
        2,
    );
    assert!(col.find_one(doc! {
        "tags": "new",
    }).unwrap().is_none());
}

#[test]
fn test_unique_multikey_empty_arrays_conflict() {
    let db = prepare_db("test-unique-multikey-empty-arrays-conflict").unwrap();
    let col = db.collection::<Document>("music");
    create_tags_index(&col, true);

    col.insert_one(doc! {
        "_id": 1,
        "tags": [],
    }).unwrap();

    let result = col.insert_one(doc! {
        "_id": 2,
        "tags": [],
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("duplicate key error"));
    assert_eq!(col.count_documents().unwrap(), 1);
}

#[test]
fn test_multikey_rejects_unsupported_array_shapes_atomically() {
    let db = prepare_db("test-multikey-rejects-unsupported-array-shapes").unwrap();
    let col = db.collection::<Document>("music");
    create_tags_index(&col, false);

    assert!(col.insert_one(doc! {
        "_id": 1,
        "tags": [
            { "name": "rock" },
        ],
    }).is_err());
    assert!(col.insert_one(doc! {
        "_id": 2,
        "tags": [
            ["rock"],
        ],
    }).is_err());
    assert_eq!(col.count_documents().unwrap(), 0);

    let ratings = db.collection::<Document>("ratings");
    ratings.create_index(IndexModel {
        keys: doc! {
            "ratings.score": 1,
        },
        options: None,
    }).unwrap();
    assert!(ratings.insert_one(doc! {
        "_id": 1,
        "ratings": [
            { "score": 5 },
        ],
    }).is_err());
    assert_eq!(ratings.count_documents().unwrap(), 0);
}
