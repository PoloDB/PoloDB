use std::rc::Rc;
use std::collections::LinkedList;
use crate::page::{PageHandler, RawPage};
use crate::btree::*;
use crate::DbResult;
use crate::bson::{Document, Value};
use crate::error::{DbErr, validation_error_reason};
use crate::db::meta_document_key;
use crate::data_ticket::DataTicket;
use std::borrow::Borrow;
use crate::index_ctx::IndexCtx;

#[derive(Clone)]
struct CursorItem {
    node:         Rc<BTreeNode>,
    index:        usize,  // pointer point to the current node
}

impl CursorItem {

    #[inline]
    fn clone_with_new_node(&self, new_node: Rc<BTreeNode>) -> CursorItem {
        CursorItem {
            node: new_node,
            index: self.index,
        }
    }

}

pub(crate) struct Cursor<'a> {
    page_handler:       &'a mut PageHandler,
    root_page_id:       u32,
    item_size:          u32,
    btree_stack:        LinkedList<CursorItem>,
    current:            Option<Rc<Document>>,
}

impl<'a> Cursor<'a> {

    pub fn new(page_handler: &mut PageHandler, root_page_id: u32) -> DbResult<Cursor> {
        let item_size = (page_handler.page_size - HEADER_SIZE) / ITEM_SIZE;

        let btree_stack = {
            let mut tmp = LinkedList::new();

            let btree_page = page_handler.pipeline_read_page(root_page_id)?;
            let btree_node = BTreeNode::from_raw(&btree_page, 0, item_size)?;

            tmp.push_back(CursorItem {
                node: Rc::new(btree_node),
                index: 0,
            });

            tmp
        };

        let mut result = Cursor {
            page_handler,
            root_page_id,
            item_size,
            btree_stack,
            current: None,
        };

        result.push_all_left_nodes()?;

        Ok(result)
    }

    fn push_all_left_nodes(&mut self) -> DbResult<()> {
        let mut top = self.btree_stack.back().unwrap().clone();
        let mut left_pid = top.node.indexes[top.index];

        while left_pid != 0 {
            let btree_page = self.page_handler.pipeline_read_page(left_pid)?;
            let btree_node = BTreeNode::from_raw(&btree_page, top.node.pid, self.item_size)?;

            self.btree_stack.push_back(CursorItem {
                node: Rc::new(btree_node),
                index: 0,
            });

            top = self.btree_stack.back().unwrap().clone();
            left_pid = top.node.indexes[top.index];
        }

        Ok(())
    }

    #[inline]
    pub fn peek(&mut self) -> Option<DataTicket> {
        if self.btree_stack.is_empty() {
            return None;
        }

        let top = self.btree_stack.back().unwrap();

        #[cfg(debug_assertions)]
        if top.node.content.is_empty() {
            panic!("top node content is empty, page_id: {}", top.node.pid);
        }

        let ticket = top.node.content[top.index].data_ticket.clone();
        Some(ticket)
    }

    pub fn update_current(&mut self, doc: &Document) -> DbResult<()> {
        let top = self.btree_stack.pop_back().unwrap();

        self.page_handler.free_data_ticket(&top.node.content[top.index].data_ticket)?;
        let key = doc.pkey_id().unwrap();
        let new_ticket = self.page_handler.store_doc(doc)?;
        let new_btree_node: BTreeNode = top.node.clone_with_content(top.index, BTreeNodeDataItem {
            key,
            data_ticket: new_ticket,
        });

        self.btree_stack.push_back(top.clone_with_new_node(Rc::new(new_btree_node)));

        self.sync_top_btree_node()
    }

    #[inline]
    fn sync_top_btree_node(&mut self) -> DbResult<()> {
        let top = self.btree_stack.back().unwrap();

        let mut page = RawPage::new(top.node.pid, self.page_handler.page_size);
        top.node.to_raw(&mut page)?;

        self.page_handler.pipeline_write_page(&page)
    }

    #[inline]
    pub fn has_next(&self) -> bool {
        !self.btree_stack.is_empty()
    }

    pub fn next(&mut self) -> DbResult<Option<Rc<Document>>> {
        if self.btree_stack.is_empty() {
            return Ok(None);
        }

        let top = self.btree_stack.pop_back().unwrap();
        let result_ticket = &top.node.content[top.index].data_ticket;
        let result = self.page_handler.get_doc_from_ticket(&result_ticket)?;

        let next_index = top.index + 1;

        if next_index >= top.node.content.len() {  // right most index
            let right_most_index = top.node.indexes[next_index];

            if right_most_index != 0 {
                self.btree_stack.push_back(CursorItem {
                    node: top.node.clone(),
                    index: next_index,
                });

                self.push_all_left_nodes()?;

                return Ok(Some(result));
            }

            // pop
            self.pop_all_right_most_item();

            return Ok(Some(result));
        }

        self.btree_stack.push_back(CursorItem {
            node: top.node.clone(),
            index: next_index,
        });

        self.push_all_left_nodes()?;

        self.current = Some(result.clone());
        Ok(Some(result))
    }

    pub fn pop_all_right_most_item(&mut self) {
        if self.btree_stack.is_empty() {
            return;
        }

        let mut top = self.btree_stack.back().unwrap();

        while top.index >= top.node.content.len() {
            self.btree_stack.pop_back();

            if self.btree_stack.is_empty() {
                return;
            }
            top = self.btree_stack.back().unwrap();
        }
    }

    pub fn insert(&mut self, col_name: &str, doc_value: Rc<Document>) -> DbResult<()> {
        let (collection_root_pid, mut meta_doc) = self.find_collection_root_pid_by_name(col_name)?;
        let meta_doc_mut = Rc::get_mut(&mut meta_doc).unwrap();

        let mut insert_wrapper = BTreePageInsertWrapper::new(self.page_handler, collection_root_pid as u32);
        let insert_result = insert_wrapper.insert_item(doc_value.borrow(), false)?;
        match &insert_result.backward_item {
            Some(backward_item) => {
                self.handle_backward_item(meta_doc_mut, collection_root_pid as u32, backward_item)?;
            }

            None => ()

        }

        let index_ctx_opt = IndexCtx::from_meta_doc(meta_doc_mut);
        match index_ctx_opt {
            Some(index_ctx) => {
                index_ctx.insert_index_by_content(doc_value.borrow(), &insert_result.data_ticket)
            }

            None => Ok(())
        }
    }

    pub fn delete(&mut self, col_name: &str, key: &Value) -> DbResult<Option<Rc<Document>>> {
        let (collection_root_pid, meta_doc) = self.find_collection_root_pid_by_name(col_name)?;

        let mut delete_wrapper = BTreePageDeleteWrapper::new(self.page_handler, collection_root_pid as u32);
        let result = delete_wrapper.delete_item(key)?;

        match &result {
            Some(deleted_item) => {
                let index_ctx_opt = IndexCtx::from_meta_doc(meta_doc.borrow());
                match index_ctx_opt {
                    Some(index_ctx) => {
                        index_ctx.delete_index_by_content(deleted_item.borrow())?;
                        Ok(result)
                    }

                    None => {
                        Ok(result)
                    }

                }
            }

            None => Ok(None)
        }

    }

    #[inline]
    fn index_already_exists(index_doc: &Document, key: &str) -> bool {
        match index_doc.get(key) {
            Some(_) => true,
            _ => false,
        }
    }

    pub fn create_index(&mut self, col_name: &str, options: Rc<Document>) -> DbResult<bool> {
        self.validate_index_options(options.borrow())?;
        let (_collection_root_pid, mut meta_doc) = self.find_collection_root_pid_by_name(col_name)?;

        let index_key = match options.get(meta_document_key::index::KEY).unwrap() {
            Value::String(content) => content,
            _ => panic!("unexpected value type")
        };

        match meta_doc.get(meta_document_key::INDEXES) {
            Some(indexes_obj) => match indexes_obj {
                Value::Document(index_doc) => {
                    if Cursor::index_already_exists(index_doc.borrow(), index_key) {
                        return Ok(false)
                    }

                    Err(DbErr::NotImplement)
                }

                _ => {
                    panic!("unexpected: indexes object is not a Document");
                }

            },

            None => {
                // create indexes
                let mut doc = Document::new_without_id();
                doc.insert(index_key.clone(), Value::Document(options));

                let mut_meta_doc = Rc::get_mut(&mut meta_doc).unwrap();
                mut_meta_doc.insert(meta_document_key::INDEXES.into(), Value::Document(Rc::new(doc)));

                Ok(true)
            }

        }
    }

    fn validate_index_options(&self, options: &Document) -> DbResult<()> {
        match options.get(meta_document_key::index::KEY) {
            Some(Value::String(_)) => (),
            _ => {
                return Err(DbErr::ValidationError(validation_error_reason::ILLEGAL_INDEX_OPTIONS_KEY.into()));
            }
        }

        match options.get(meta_document_key::index::NAME) {
            None |
            Some(Value::String(_)) => (),

            _ => {
                return Err(DbErr::ValidationError(validation_error_reason::TYPE_OF_INDEX_NAME_SHOULD_BE_STRING.into()));
            }
        }

        match options.get(meta_document_key::index::V) {
            Some(Value::Int(1)) => (),

            _ => {
                return Err(DbErr::ValidationError(validation_error_reason::ORDER_OF_INDEX_CAN_ONLY_BE_ONE.into()));
            }
        }

        match options.get(meta_document_key::index::UNIQUE) {
            Some(Value::Boolean(_)) => (),

            Some(_) => {
                return Err(DbErr::ValidationError(validation_error_reason::UNIQUE_PROP_SHOULD_BE_BOOLEAN.into()));
            }

            None => (),
        }

        Ok(())
    }

    fn find_collection_root_pid_by_name(&mut self, col_name: &str) -> DbResult<(i64, Rc<Document>)> {
        while self.has_next() {
            let ticket = self.peek().unwrap();
            let doc = self.page_handler.get_doc_from_ticket(&ticket)?;
            match doc.get(meta_document_key::NAME) {
                Some(Value::String(name)) => {
                    if name == col_name {  // found
                        let page_id = doc.get(meta_document_key::ROOT_PID).unwrap();
                        match page_id {
                            Value::Int(page_id) => {
                                return Ok((*page_id, doc.clone()))
                            }

                            _ => panic!("page id is not int type")
                        }
                    }
                }

                _ => ()
            }

            let _ = self.next()?;
        }

        Err(DbErr::CollectionNotFound(col_name.into()))
    }

    #[inline]
    pub(crate) fn get_doc_from_ticket(&mut self, ticket: &DataTicket) -> DbResult<Rc<Document>> {
        self.page_handler.get_doc_from_ticket(ticket)
    }

    fn handle_backward_item(&mut self, meta_doc_mut: &mut Document, left_pid: u32, backward_item: &InsertBackwardItem) -> DbResult<()> {
        let new_root_id = self.page_handler.alloc_page_id()?;

        #[cfg(feature = "log")]
        eprintln!("handle backward item, left_pid: {}, new_root_id: {}, right_pid: {}", left_pid, new_root_id, backward_item.right_pid);

        let new_root_page = backward_item.write_to_page(self.page_handler, new_root_id, left_pid)?;

        meta_doc_mut.insert(meta_document_key::ROOT_PID.into(), Value::Int(new_root_id as i64));
        self.update_current(meta_doc_mut)?;

        self.page_handler.pipeline_write_page(&new_root_page)
    }

}
