use std::rc::Rc;
use std::collections::HashMap;
use std::borrow::Borrow;
use polodb_bson::{Document, Value};
use crate::meta_doc_helper::{meta_doc_key, MetaDocEntry};
use crate::DbResult;
use crate::error::{DbErr, mk_field_name_type_unexpected};
use crate::page::PageHandler;
use crate::btree::{BTreePageInsertWrapper, InsertBackwardItem, BTreePageDeleteWrapper};

pub(crate) struct IndexCtx {
    key_to_entry: HashMap<String, IndexEntry>,
}

impl IndexCtx {

    pub fn new() -> IndexCtx {
        IndexCtx {
            key_to_entry: HashMap::new(),
        }
    }

    // indexes:
    //     key -> index_entry
    pub fn from_meta_doc(doc: &Document) -> Option<IndexCtx> {
        let indexes = doc.get(meta_doc_key::INDEXES)?;

        let meta_doc: &Rc<Document> = indexes.unwrap_document();
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

    pub fn merge_to_meta_doc(&self, collection_meta: &mut MetaDocEntry) {
        let mut new_back_doc = Document::new_without_id();
        for (key, entry) in &self.key_to_entry {
            let index_meta_doc = Rc::new(entry.to_doc());
            new_back_doc.insert(key.clone(), Value::Document(index_meta_doc));
        }

        collection_meta.set_indexes(new_back_doc);
    }

    pub fn insert_index_by_content(&mut self, doc: &Document, primary_key: &Value, is_ctx_changed: &mut bool, page_handler: &mut PageHandler) -> DbResult<()> {
        for (key, entry) in &mut self.key_to_entry {
            if let Some(value) = doc.get(key) {
                // index exist, and value exist
                entry.insert_index(value, primary_key.clone(), is_ctx_changed, page_handler)?;
            }
        }

        Ok(())
    }

    pub fn delete_index_by_content(&self, doc: &Document, page_handler: &mut PageHandler) -> DbResult<()> {
        for (key, entry) in &self.key_to_entry {
            if let Some(value) = doc.get(key) {
                entry.remove_index(value, page_handler)?;
            }
        }

        Ok(())
    }

}

struct IndexEntry {
    name:     Option<String>,
    unique:   bool,
    root_pid: u32,
}

impl IndexEntry {

    fn from_option_doc(doc: &Document) -> IndexEntry {
        let name = doc.get(meta_doc_key::index::NAME).map(|val| {
            val.unwrap_string().to_string()
        });
        let unique = doc.get(meta_doc_key::index::UNIQUE).unwrap().unwrap_boolean();
        let root_pid = doc.get(meta_doc_key::index::ROOT_PID).unwrap().unwrap_int();

        IndexEntry {
            name,
            unique,
            root_pid: root_pid as u32,
        }
    }

    fn to_doc(&self) -> Document {
        let mut result = Document::new_without_id();
        if let Some(name_val) = &self.name {
            result.insert(meta_doc_key::index::NAME.into(), Value::String(name_val.as_str().into()));
        }
        result.insert(meta_doc_key::index::UNIQUE.into(), Value::Boolean(self.unique));
        result.insert(meta_doc_key::index::ROOT_PID.into(), Value::Int(self.root_pid as i64));
        result
    }

    // store (data_value -> primary_key)
    fn insert_index(
        &mut self, data_value: &Value, primary_key: Value,
        is_changed: &mut bool,
        page_handler: &mut PageHandler) -> DbResult<()> {

        if !data_value.is_valid_key_type() {
            return Err(DbErr::NotAValidKeyType(data_value.ty_name().into()));
        }

        let mut insert_wrapper = BTreePageInsertWrapper::new(page_handler, self.root_pid);

        let mut index_entry_doc = IndexEntry::mk_index_entry_doc(data_value, primary_key);

        let insert_result = insert_wrapper.insert_item(&index_entry_doc, false)?;

        if let Some(backward_item) = &insert_result.backward_item {
            *is_changed = true;
            return self.handle_backward_item(&mut index_entry_doc, backward_item, page_handler)
        }

        Ok(())
    }

    fn handle_backward_item(&mut self, meta_doc: &mut Document, backward_item: &InsertBackwardItem, page_handler: &mut PageHandler) -> DbResult<()> {
        let new_root_id = page_handler.alloc_page_id()?;

        crate::polo_log!("index handle backward item, left_pid: {}, new_root_id: {}, right_pid: {}", self.root_pid, new_root_id, backward_item.right_pid);

        let new_root_page = backward_item.write_to_page(page_handler, new_root_id, self.root_pid)?;

        meta_doc.insert(meta_doc_key::index::ROOT_PID.into(), Value::Int(new_root_id as i64));

        self.root_pid = new_root_id;

        page_handler.pipeline_write_page(&new_root_page)
    }

    fn mk_index_entry_doc(data_value: &Value, primary_key: Value) -> Document {
        let mut doc = Document::new_without_id();
        doc.insert("_id".into(), data_value.clone());

        doc.insert("pkey".into(), primary_key);

        doc
    }

    fn remove_index(&self, data_value: &Value, page_handler: &mut PageHandler) -> DbResult<()> {
        let mut delete_wrapper = BTreePageDeleteWrapper::new(page_handler, self.root_pid);
        let _result = delete_wrapper.delete_item(data_value)?;
        Ok(())
    }

}

macro_rules! match_and_merge_option {
    ($options:expr, $key_name:expr, $target: expr, $val_ty: tt) => {
        match $options.get($key_name) {
            Some(Value::$val_ty(val)) => {
                $target.insert($key_name.into(), Value::$val_ty(val.clone()));
            }

            Some(val) => {
                let err = mk_field_name_type_unexpected($key_name, stringify!($val_ty), val.ty_name());
                return Err(err)
            }

            None => ()

        }
    };
}

#[inline]
fn mk_default_index_options() -> Document {
    let mut result = Document::new_without_id();

    result.insert(meta_doc_key::index::UNIQUE.into(), Value::Boolean(false));
    result.insert(meta_doc_key::index::V.into(), Value::Int(1));

    result
}

pub(crate) fn merge_options_into_default(root_pid: u32, options: Option<&Document>) -> DbResult<Document> {
    let mut doc = mk_default_index_options();

    doc.insert(meta_doc_key::index::ROOT_PID.into(), Value::Int(root_pid as i64));

    if let Some(options) = options {
        match_and_merge_option!(options, meta_doc_key::index::NAME, doc, String);
        match_and_merge_option!(options, meta_doc_key::index::V, doc, Int);
        match_and_merge_option!(options, meta_doc_key::index::UNIQUE, doc, Boolean);
    }

    Ok(doc)
}
