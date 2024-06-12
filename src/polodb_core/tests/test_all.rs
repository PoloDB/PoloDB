use bson::{doc, Document};
use polodb_core::Database;

mod common;

use common::prepare_db;

#[test]
fn test_all() {
    vec![
        (prepare_db("test-all").unwrap(), true),
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
                "_id": "invalid",
                "value": ["c1", "c2", "c4"],
            },
        ];
        collection.insert_many(&docs).unwrap();

        let res = collection
            .find(doc! {
                "value": {
                    "$all": ["c2", "c3"],
                }
            })
            .unwrap();

        assert_eq!(res.count(), docs.len() - 1);
    });
}

#[test]
fn test_all_error() {
    vec![
        (prepare_db("test-all-error").unwrap(), true),
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
                "arr": ["c1", "c2", "c3"],
            },
            doc! {
                "_id": "invalid2",
                "value": "not-valid-value",
            },
        ];
        collection.insert_many(&docs).unwrap();

        let mut res = collection
            .find(doc! {
                "value": {
                    "$all": ["c2", "c3"],
                }
            })
            .unwrap();

        assert!(res.next().unwrap().is_err());
    });
}
