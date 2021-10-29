use std::path::{Path, PathBuf};
use std::rc::Rc;
use polodb_bson::Document;
use crate::{DbResult, Config, SerializeType, DbContext, TransactionType, Database};

fn mk_old_db_path(db_path: &Path) -> PathBuf {
    let mut buf = db_path.to_path_buf();
    let filename = buf.file_name().unwrap().to_str().unwrap();
    let new_filename = String::from(filename) + ".old";
    buf.set_file_name(new_filename);
    buf
}

fn mk_new_db_path(db_path: &Path) -> PathBuf {
    let mut buf = db_path.to_path_buf();
    let filename = buf.file_name().unwrap().to_str().unwrap();
    let new_filename = String::from(filename) + ".new";
    buf.set_file_name(new_filename);
    buf
}

fn find_all(db_ctx: &mut DbContext, col_id: u32, meta_version: u32) -> DbResult<Vec<Rc<Document>>> {
    let mut result = vec![];

    let mut handle = db_ctx.find(col_id, meta_version, None)?;

    handle.step()?;

    while handle.has_row() {
        let doc = handle.get().unwrap_document();
        result.push(doc.clone());

        handle.step()?;
    }

    Ok(result)
}

fn do_transfer(db_ctx: &mut DbContext, new_db: &mut Database) -> DbResult<()> {
    let vec = db_ctx.query_all_meta()?;

    println!("size: {}", vec.len());

    for item in vec {
        println!("hello: {}", item);
        let id = item.get("_id").unwrap().unwrap_int() as u32;
        let name = item.get("name").expect("not a valid db").unwrap_string();

        let data = find_all(db_ctx, id, db_ctx.meta_version).unwrap();

        let mut new_collection = new_db.create_collection(name).unwrap();

        for item in data {
            let mut doc = item.as_ref().clone();
            new_collection.insert(&mut doc).unwrap();
        }
    }

    Ok(())
}

pub(crate) fn v1_to_v2(path: &Path) -> DbResult<()> {
    let new_db_path = mk_new_db_path(path);
    let old_db_path = mk_old_db_path(path);

    let result = {
        let mut new_db = Database::open_file(&new_db_path)?;

        new_db.start_transaction(Some(TransactionType::Write))?;

        let mut config = Config::default();
        config.serialize_type = SerializeType::Legacy;
        let mut db_ctx= crate::context::DbContext::open_file(&path, config)?;

        db_ctx.start_transaction(Some(TransactionType::Read))?;

        let result = do_transfer(&mut db_ctx, &mut new_db);

        new_db.commit()?;
        db_ctx.commit()?;

        result
    };

    if result.is_ok()  {
        std::fs::rename(path, old_db_path)?;
        std::fs::rename(&new_db_path, path)?;
    } else {
        let _ = std::fs::remove_file(&new_db_path);
    }

    return result;
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use crate::Database;
    use crate::migration::v1_to_v2;

    fn mk_db_path(db_name: &str) -> PathBuf {
        let mut db_path = std::env::temp_dir();
        let db_filename = String::from(db_name) + ".db";
        db_path.push(db_filename);
        db_path
    }

    #[test]
    fn test_meta_information() {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.pop();
        d.pop();
        d.push("fixtures/test-collection.db");

        let test_path = mk_db_path("test-migration");
        let _ = std::fs::remove_file(&test_path);

        std::fs::copy(&d, &test_path).unwrap();

        println!("path: {}", d.to_str().unwrap());

        v1_to_v2(&test_path).unwrap();

        let mut new_db = Database::open_file(&test_path).unwrap();
        let meta = new_db.query_all_meta().unwrap();
        assert!(meta.len()> 0);
    }

}
