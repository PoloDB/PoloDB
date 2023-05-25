use bson::{doc, Document, Regex};
use polodb_core::Database;

mod common;

use common::prepare_db;

#[test]
fn test_regex() {
    vec![
        (prepare_db("test-regex").unwrap(), true),
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
                "value": "c1",
            },
            doc! {
                "_id": "invalid",
                "value": "not-valid-value",
            },
            doc! {
                "_id": "c3",
                "value": "c3"
            },
        ];
        collection.insert_many(&docs).unwrap();

        let res = collection
            .find(doc! {
                "value": {
                    "$regex": Regex {
                        pattern: "c[0-9]+".into(),
                        options: "i".into(),
                    },
                }
            })
            .unwrap();

        assert_eq!(res.count(), docs.len() - 1);
    });
}

#[test]
fn test_regex_error() {
    vec![
        (prepare_db("test-regex-error").unwrap(), true),
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
                "value": "c1",
            },
            doc! {
                "_id": "invalid",
                "value": "not-valid-value",
            },
            doc! {
                "_id": "c3",
                "value": "c3"
            },
        ];
        collection.insert_many(&docs).unwrap();

        let mut res = collection
            .find(doc! {
                "value": {
                    "$regex": Regex {
                        pattern: "c[0-9]+".into(),
                        options: "pml".into(), // invalid option
                    },
                }
            })
            .unwrap();

        assert!(res.next().unwrap().is_err());
    });
}
