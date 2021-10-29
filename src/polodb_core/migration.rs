use crate::{DbResult, Config, DbContext, TransactionType, SerializeType};
use std::path::{Path, PathBuf};
use crate::meta_doc_helper::MetaDocEntry;
use crate::vm::SubProgram;
use polodb_bson::Document;

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

fn get_all_meta_doc(ctx: &mut DbContext) -> DbResult<Vec<Document>> {
    ctx.start_transaction(Some(TransactionType::Read))?;

    let mut result = vec![];
    let meta_src = ctx.get_meta_source()?;
    let collection_meta = MetaDocEntry::new(0, "<meta>".into(), meta_src.meta_pid);
    let subprogram = SubProgram::compile_query_all(
        &collection_meta,
        true)?;

    let mut handle = ctx.make_handle(subprogram);
    handle.step()?;

    Ok(result)
}

pub(crate) fn v1_to_v2(path: &Path) -> DbResult<()> {
    let new_db_path = mk_new_db_path(path);
    let old_db_path = mk_old_db_path(path);

    let mut config = Config::default();
    config.serialize_type = SerializeType::Legacy;
    let mut db_ctx= crate::context::DbContext::open_file(&new_db_path, config)?;

    let _ = get_all_meta_doc(&mut db_ctx)?;

    std::fs::rename(path, old_db_path)?;
    std::fs::rename(new_db_path, path)?;

    Ok(())
}
