use bson::{doc, Document};
use polodb_core::Database;

mod common;

use common::prepare_db;

#[test]
fn test_in_array() {
    vec![
        (prepare_db("test-all-array").unwrap(), true),
        (Database::open_memory().unwrap(), false),
    ]
    .iter()
    .for_each(|(db, _)| {
        let metrics = db.metrics();
        metrics.enable();

        let collection = db.collection::<Document>("config");
        let docs = vec![
            doc! {
                "_id": "c1",
                "value": ["c1", "c2", "c3"],
            },
            doc! {
                "_id": "c2",
                "value": ["c2", "c4"],
            },
            doc! {
                "_id": "invalid",
                "value": ["c5", "c6", "c4"],
            },
        ];
        collection.insert_many(&docs).unwrap();

        let res = collection
            .find(doc! {
                "value": {
                    "$in": ["c2", "c52"],
                }
            })
            .unwrap();

        assert_eq!(res.count(), docs.len() - 1);
    });
}

#[test]
fn test_in() {
    vec![
        (prepare_db("test-in").unwrap(), true),
        (Database::open_memory().unwrap(), false),
    ]
    .iter()
    .for_each(|(db, _)| {
        let metrics = db.metrics();
        metrics.enable();

        let collection = db.collection::<Document>("config");
        let docs = vec![
            doc! {
                "_id": "c1",
                "value": 18,
            },
            doc! {
                "_id": "invalid",
                "value": 50,
            },
        ];
        collection.insert_many(&docs).unwrap();

        let res = collection
            .find(doc! {
                "value": {
                    "$in": [20, 18, 30],
                }
            })
            .unwrap();

        assert_eq!(res.count(), docs.len() - 1);
    });
}
