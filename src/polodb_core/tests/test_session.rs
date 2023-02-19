use polodb_core::{Database, DbErr};
use polodb_core::bson::{Document, doc};

mod common;

use common::prepare_db;

#[test]
fn test_transaction_commit() {
    vec![
        prepare_db("test-transaction-commit").unwrap(),
        Database::open_memory().unwrap(),
    ].iter().for_each(|db| {
        let mut session = db.start_session().unwrap();
        session.start_transaction(None).unwrap();

        let collection = db.collection::<Document>("test");

        for i in 0..10 {
            let content = i.to_string();
            let mut new_doc = doc! {
                "_id": i,
                "content": content,
            };
            collection.insert_one_with_session(&mut new_doc, &mut session).unwrap();
        }

        session.commit_transaction().unwrap();

        let doc = collection.find_many(doc! {}).unwrap();
        assert_eq!(doc.len(), 10);
    });
}

#[test]
fn test_session_outdated() {
    let db = prepare_db("test-session-outdate").unwrap();
    let col = db.collection::<Document>("test");

    let mut session = db.start_session().unwrap();
    session.start_transaction(None).unwrap();

    col.insert_one(doc! {
        "name": "Vincent",
    }).unwrap();

    col.insert_one_with_session(doc! {
        "name": "Vincent",
    }, &mut session).unwrap();

    let result = session.commit_transaction();
    assert!(match result {
        Err(DbErr::SessionOutdated) => true,
        _ => false,
    })
}
