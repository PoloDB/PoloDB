/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::path::PathBuf;
use std::env;
use polodb_core::{Config, Database, Result};
use polodb_core::bson::{Document, doc};

#[allow(dead_code)]
pub fn mk_db_path(db_name: &str) -> PathBuf {

    let mut db_path = env::temp_dir();
    let db_filename = String::from(db_name) + "-db";
    db_path.push(db_filename);
    db_path
}

#[allow(dead_code)]
pub fn prepare_db_with_config(db_name: &str, config: Config) -> Result<Database> {
    let db_path = mk_db_path(db_name);

    let _ = std::fs::remove_dir_all(db_path.as_path());

    Database::open_file_with_config(db_path.as_path().to_str().unwrap(), config)
}

#[allow(dead_code)]
pub fn clean_db_path(db_path: &str) {
    let _ = std::fs::remove_dir_all(db_path);
}

#[allow(dead_code)]
pub fn prepare_db(db_name: &str) -> Result<Database> {
    prepare_db_with_config(db_name, Config::default())
}

fn insert_items_to_db(db: Database, size: usize) -> Database {
    let collection = db.collection::<Document>("test");

    let mut data: Vec<Document> = vec![];

    for i in 0..size {
        let content = i.to_string();
        let new_doc = doc! {
            "content": content,
        };
        data.push(new_doc);
    }

    collection.insert_many(&data).unwrap();

    db
}

#[allow(dead_code)]
pub fn create_file_and_return_db_with_items(db_name: &str, size: usize) -> Database {
    let db = prepare_db(db_name).unwrap();
    insert_items_to_db(db, size)
}
