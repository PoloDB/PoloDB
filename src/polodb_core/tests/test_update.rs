use polodb_core::Database;
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

