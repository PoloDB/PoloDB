use std::path::PathBuf;
use std::env;
use crate::{Config, Database, DbResult};

pub fn mk_db_path(db_name: &str) -> PathBuf {

    let mut db_path = env::temp_dir();
    let db_filename = String::from(db_name) + ".db";
    db_path.push(db_filename);
    db_path
}

pub fn mk_journal_path(db_name: &str) -> PathBuf {
    let mut journal_path = env::temp_dir();

    let journal_filename = String::from(db_name) + ".db.journal";
    journal_path.push(journal_filename);

    journal_path
}

pub fn prepare_db_with_config(db_name: &str, config: Config) -> DbResult<Database> {
    let db_path = mk_db_path(db_name);
    let journal_path = mk_journal_path(db_name);

    let _ = std::fs::remove_file(db_path.as_path());
    let _ = std::fs::remove_file(journal_path);

    Database::open_file_with_config(db_path.as_path().to_str().unwrap(), config)
}

pub fn prepare_db(db_name: &str) -> DbResult<Database> {
    prepare_db_with_config(db_name, Config::default())
}