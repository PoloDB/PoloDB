/*
 * Copyright (c) 2020 Vincent Chan
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation; either version 3, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */
use std::rc::Rc;
use std::collections::HashMap;
use std::cell::Cell;
use std::borrow::Borrow;
use crate::db::meta_document_key;
use crate::bson::{Document, Value};
use crate::DbResult;
use crate::error::{DbErr, mk_index_options_type_unexpected};
use crate::data_ticket::DataTicket;
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
        let indexes_opt = doc.get(meta_document_key::INDEXES);
        if indexes_opt.is_none() {  // no indexes
            return None;
        }

        let meta_doc: &Rc<Document> = indexes_opt.unwrap().unwrap_document();
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

    pub fn merge_to_meta_doc(&self, meta_doc: &mut Document) {
        let mut new_back_doc = Document::new_without_id();
        for (key, entry) in &self.key_to_entry {
            let index_meta_doc = Rc::new(entry.to_doc());
            new_back_doc.insert(key.clone(), Value::Document(index_meta_doc));
        }

        meta_doc.insert(meta_document_key::INDEXES.into(), Value::Document(Rc::new(new_back_doc)));
    }

    pub fn insert_index_by_content(&mut self, doc: &Document, data_ticket: &DataTicket, is_ctx_changed: &Cell<bool>, page_handler: &mut PageHandler) -> DbResult<()> {
        for (key, entry) in &mut self.key_to_entry {
            if let Some(value) = doc.get(key) {
                // index exist, and value exist
                entry.insert_index(value, data_ticket, is_ctx_changed, page_handler)?;
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

    fn to_doc(&self) -> Document {
        let mut result = Document::new_without_id();
        result.insert(meta_document_key::index::NAME.into(), Value::String(Rc::new(self.name.clone())));
        result.insert(meta_document_key::index::UNIQUE.into(), Value::Boolean(self.unique));
        result.insert(meta_document_key::index::ROOT_PID.into(), Value::Int(self.root_pid as i64));
        result
    }

    // store (data_value -> data_ticket)
    fn insert_index(
        &mut self, data_value: &Value, data_ticket: &DataTicket,
        is_changed: &Cell<bool>,
        page_handler: &mut PageHandler) -> DbResult<()> {

        if !data_value.is_valid_key_type() {
            return Err(DbErr::NotAValidKeyType(data_value.ty_name().into()));
        }

        let mut insert_wrapper = BTreePageInsertWrapper::new(page_handler, self.root_pid);

        let mut index_entry_doc = IndexEntry::mk_index_entry_doc(data_value, data_ticket);

        let insert_result = insert_wrapper.insert_item(&index_entry_doc, false)?;

        if let Some(backward_item) = &insert_result.backward_item {
            is_changed.set(true);
            return self.handle_backward_item(&mut index_entry_doc, backward_item, page_handler)
        }

        Ok(())
    }

    fn handle_backward_item(&mut self, meta_doc: &mut Document, backward_item: &InsertBackwardItem, page_handler: &mut PageHandler) -> DbResult<()> {
        let new_root_id = page_handler.alloc_page_id()?;

        #[cfg(feature = "log")]
        eprintln!("index handle backward item, left_pid: {}, new_root_id: {}, right_pid: {}", self.root_pid, new_root_id, backward_item.right_pid);

        let new_root_page = backward_item.write_to_page(page_handler, new_root_id, self.root_pid)?;

        meta_doc.insert(meta_document_key::index::ROOT_PID.into(), Value::Int(new_root_id as i64));

        self.root_pid = new_root_id;

        page_handler.pipeline_write_page(&new_root_page)
    }

    fn mk_index_entry_doc(data_value: &Value, data_ticket: &DataTicket) -> Document {
        let mut doc = Document::new_without_id();
        doc.insert("_id".into(), data_value.clone());

        let data_ticket_bytes = data_ticket.to_bytes().to_vec();
        doc.insert("value".into(), Value::Binary(Rc::new(data_ticket_bytes)));

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
                let err = mk_index_options_type_unexpected($key_name, stringify!($val_ty), val.ty_name());
                return Err(err)
            }

            None => ()

        }
    };
}

#[inline]
fn mk_default_index_options() -> Document {
    let mut result = Document::new_without_id();

    result.insert(meta_document_key::index::UNIQUE.into(), Value::Boolean(false));
    result.insert(meta_document_key::index::V.into(), Value::Int(1));

    result
}

pub(crate) fn merge_options_into_default(root_pid: u32, options: Option<&Document>) -> DbResult<Document> {
    let mut doc = mk_default_index_options();

    doc.insert(meta_document_key::index::ROOT_PID.into(), Value::Int(root_pid as i64));

    match options {
        Some(options) => {
            match_and_merge_option!(options, meta_document_key::index::NAME, doc, String);
            match_and_merge_option!(options, meta_document_key::index::V, doc, Int);
            match_and_merge_option!(options, meta_document_key::index::UNIQUE, doc, Boolean);
        }

        None => ()
    }

    Ok(doc)
}
