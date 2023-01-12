use std::path::PathBuf;
use std::env;

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
