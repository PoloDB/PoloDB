use bson::{doc, Document};
use polodb_core::{Database, Result};

#[test]
fn test_aggregate_empty() {
    let db = Database::open_memory().unwrap();
    let fruits = db.collection::<Document>("fruits");
    fruits.insert_many(vec![
        doc! {
            "name": "apple",
            "color": "red",
            "shape": "round",
        },
        doc! {
            "name": "banana",
            "color": "yellow",
            "shape": "long",
        },
        doc! {
            "name": "orange",
            "color": "orange",
            "shape": "round",
        },
    ]).unwrap();

    let result = fruits
        .aggregate(vec![])
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 3);
}

#[test]
fn test_aggregate_match() {
    let db = Database::open_memory().unwrap();
    let fruits = db.collection::<Document>("fruits");
    fruits.insert_many(vec![
        doc! {
            "name": "apple",
            "color": "red",
            "shape": "round",
        },
        doc! {
            "name": "banana",
            "color": "yellow",
            "shape": "long",
        },
        doc! {
            "name": "orange",
            "color": "orange",
            "shape": "round",
        },
        doc! {
            "name": "pear",
            "color": "yellow",
            "shape": "round",
        },
        doc! {
            "name": "peach",
            "color": "orange",
            "shape": "round",
        },
    ]).unwrap();

    let result = fruits
        .aggregate(vec![
            doc! {
                "$match": {
                    "color": "yellow",
                },
            }
        ])
        .unwrap()
        .collect::<Result<Vec<Document>>>()
        .unwrap();
    assert_eq!(result.len(), 2);

    assert_eq!(result[0].get("name").unwrap().as_str().unwrap(), "banana");
    assert_eq!(result[1].get("name").unwrap().as_str().unwrap(), "pear");
}