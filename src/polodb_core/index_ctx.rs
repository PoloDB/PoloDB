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
        for (key, _entry) in &self.key_to_entry {
            match doc.get(key) {
                Some(_value) => {

                }

                None => {

                }

            }


        }
        Err(DbErr::NotImplement)
    }

    pub fn delete_index_by_content(&self, _doc: &Document) -> DbResult<()> {
        Err(DbErr::NotImplement)
    }

}

struct IndexEntry {
    unique: bool,
}

impl IndexEntry {

    fn from_option_doc(doc: &Document) -> IndexEntry {
        let mut unique = false;

        match doc.get(meta_document_key::index::UNIQUE) {
            Some(Value::Boolean(bl)) => {
                unique = *bl;
            }

            _ => ()
        }

        IndexEntry {
            unique,
        }
    }

}
