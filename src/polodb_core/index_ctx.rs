use std::rc::Rc;
use std::collections::HashMap;
use crate::db::meta_document_key;
use crate::bson::{Document, Value};
use std::borrow::Borrow;
use crate::DbResult;
use crate::error::DbErr;
use crate::data_ticket::DataTicket;

pub(crate) struct IndexCtx {
    key_to_entry: HashMap<String, IndexEntry>,
}

impl IndexCtx {

    pub fn new() -> IndexCtx {
        IndexCtx {
            key_to_entry: HashMap::new(),
        }
    }

    pub fn from_meta_doc(doc: &Document) -> Option<IndexCtx> {
        let value = doc.get(meta_document_key::INDEXES);
        if value.is_none() {  // no indexes
            return None;
        }

        let meta_doc: &Rc<Document> = value.unwrap().unwrap_document();
        if meta_doc.is_empty() {
            return None;
        }

        let mut result = IndexCtx::new();

        for (key, options) in meta_doc.iter() {
            let options_doc = options.unwrap_document();
            let entry = IndexEntry::from_option_doc(options_doc.borrow());
            result.key_to_entry.insert(key.clone(), entry);
        }

        Some(result)
    }

    pub fn insert_index_by_content(&self, doc: &Document, _data_ticket: &DataTicket) -> DbResult<()> {
        for (key, entry) in &self.key_to_entry {
            if let Some(value) = doc.get(key) {
                entry.insert_index(value)?;
            }

        }

        Ok(())
    }

    pub fn delete_index_by_content(&self, _doc: &Document) -> DbResult<()> {
        Err(DbErr::NotImplement)
    }

}

struct IndexEntry {
    name:     String,
    unique:   bool,
    root_pid: u32,
}

impl IndexEntry {

    fn from_option_doc(doc: &Document) -> IndexEntry {
        let name = doc.get(meta_document_key::index::NAME).unwrap().unwrap_string();
        let unique = doc.get(meta_document_key::index::UNIQUE).unwrap().unwrap_boolean();
        let root_pid = doc.get(meta_document_key::index::ROOT_PID).unwrap().unwrap_int();

        IndexEntry {
            name: name.into(),
            unique,
            root_pid: root_pid as u32,
        }
    }

    fn insert_index(&self, value: &Value) -> DbResult<()> {
        if !value.is_valid_key_type() {
            return Err(DbErr::NotAValidKeyType(value.ty_name().into()));
        }
        Err(DbErr::NotImplement)
    }

}
