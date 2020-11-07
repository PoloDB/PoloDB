use std::rc::Rc;
use std::collections::LinkedList;
use polodb_bson::{Document, Value};
use crate::page::{PageHandler, RawPage};
use crate::btree::*;
use crate::DbResult;
use crate::data_ticket::DataTicket;

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

pub(crate) struct Cursor {
    root_pid:           u32,
    item_size:          u32,
    btree_stack:        LinkedList<CursorItem>,
    current:            Option<Rc<Document>>,
}

impl Cursor {

    pub fn new(item_size: u32, root_pid: u32) -> Cursor {
        Cursor {
            root_pid,
            item_size,
            btree_stack: LinkedList::new(),
            current: None,
        }
    }

    pub fn reset(&mut self, page_handler: &mut PageHandler) -> DbResult<()> {
        self.mk_initial_btree(page_handler, self.root_pid, self.item_size)?;

        if self.btree_stack.is_empty() {
            return Ok(());
        }

        self.push_all_left_nodes(page_handler)?;

        Ok(())
    }

    pub fn reset_by_pkey(&mut self, page_handler: &mut PageHandler, pkey: &Value) -> DbResult<bool> {
        self.btree_stack.clear();

        let mut current_pid = self.root_pid;
        let item_size = self.item_size;

        // recursively find the item
        while current_pid > 0 {
            let btree_page = page_handler.pipeline_read_page(current_pid)?;
            let btree_node = BTreeNode::from_raw(
                &btree_page, 0,
                item_size,
                page_handler
            )?;

            if btree_node.is_empty() {
                return Ok(false);
            }

            let search_result = btree_node.search(pkey)?;
            match search_result {
                SearchKeyResult::Node(index) => {
                    self.btree_stack.push_back(CursorItem {
                        node: Rc::new(btree_node),
                        index,
                    });
                    return Ok(true)
                }

                SearchKeyResult::Index(index) => {
                    let next_pid = btree_node.indexes[index];
                    if next_pid == 0 {
                        return Ok(false);
                    }

                    self.btree_stack.push_back(CursorItem {
                        node: Rc::new(btree_node),
                        index
                    });

                    current_pid = next_pid;
                }

            }
        }

        Ok(false)
    }

    fn mk_initial_btree(&mut self, page_handler: &mut PageHandler, root_page_id: u32, item_size: u32) -> DbResult<()> {
        self.btree_stack.clear();

        let btree_page = page_handler.pipeline_read_page(root_page_id)?;
        let btree_node = BTreeNode::from_raw(
            &btree_page, 0,
            item_size,
            page_handler
        )?;

        if !btree_node.content.is_empty() {
            self.btree_stack.push_back(CursorItem {
                node: Rc::new(btree_node),
                index: 0,
            });
        }

        Ok(())
    }

    fn push_all_left_nodes(&mut self, page_handler: &mut PageHandler) -> DbResult<()> {
        if self.btree_stack.is_empty() {
            return Ok(());
        }
        let mut top = self.btree_stack.back().unwrap().clone();
        let mut left_pid = top.node.indexes[top.index];

        while left_pid != 0 {
            let btree_page = page_handler.pipeline_read_page(left_pid)?;
            let btree_node = BTreeNode::from_raw(
                &btree_page,
                top.node.pid,
                self.item_size,
                page_handler
            )?;

            self.btree_stack.push_back(CursorItem {
                node: Rc::new(btree_node),
                index: 0,
            });

            top = self.btree_stack.back().unwrap().clone();
            left_pid = top.node.indexes[top.index];
        }

        Ok(())
    }

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

    pub fn update_current(&mut self, page_handler: &mut PageHandler, doc: &Document) -> DbResult<()> {
        let top = self.btree_stack.pop_back().unwrap();

        page_handler.free_data_ticket(&top.node.content[top.index].data_ticket)?;
        let key = doc.pkey_id().unwrap();
        let new_ticket = page_handler.store_doc(doc)?;
        let new_btree_node: BTreeNode = top.node.clone_with_content(
            top.index,
            BTreeNodeDataItem {
                key,
                data_ticket: new_ticket,
            });

        self.btree_stack.push_back(
            top.clone_with_new_node(Rc::new(new_btree_node)));

        self.sync_top_btree_node(page_handler)
    }

    fn sync_top_btree_node(&mut self, page_handler: &mut PageHandler) -> DbResult<()> {
        let top = self.btree_stack.back().unwrap();

        let mut page = RawPage::new(top.node.pid, page_handler.page_size);
        top.node.to_raw(&mut page)?;

        page_handler.pipeline_write_page(&page)
    }

    #[inline]
    pub fn has_next(&self) -> bool {
        !self.btree_stack.is_empty()
    }

    pub fn next(&mut self, page_handler: &mut PageHandler) -> DbResult<Option<Rc<Document>>> {
        if self.btree_stack.is_empty() {
            return Ok(None);
        }

        let top = self.btree_stack.pop_back().unwrap();
        let result_ticket = &top.node.content[top.index].data_ticket;
        let result = page_handler.get_doc_from_ticket(&result_ticket)?.unwrap();

        let next_index = top.index + 1;

        if next_index >= top.node.content.len() {  // right most index
            let right_most_index = top.node.indexes[next_index];

            if right_most_index != 0 {
                self.btree_stack.push_back(CursorItem {
                    node: top.node.clone(),
                    index: next_index,
                });

                self.push_all_left_nodes(page_handler)?;

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

        self.push_all_left_nodes(page_handler)?;

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

    // pub fn insert(&mut self, col_name: &str, doc_value: Rc<Document>) -> DbResult<()> {
    //     let (collection_root_pid, mut meta_doc) = self.find_collection_root_pid_by_name(col_name)?;
    //     let meta_doc_mut = Rc::get_mut(&mut meta_doc).unwrap();
    //
    //     let mut insert_wrapper = BTreePageInsertWrapper::new(
    //         self.page_handler, collection_root_pid as u32);
    //     let insert_result = insert_wrapper.insert_item(doc_value.borrow(), false)?;
    //
    //     if let Some(backward_item) = &insert_result.backward_item {
    //         self.handle_backward_item(meta_doc_mut, collection_root_pid as u32, backward_item)?;
    //     }
    //
    //     let mut index_ctx_opt = IndexCtx::from_meta_doc(meta_doc_mut);
    //     if let Some(index_ctx) = &mut index_ctx_opt {
    //         let is_ctx_changed: Cell<bool> = Cell::new(false);
    //
    //         index_ctx.insert_index_by_content(
    //             doc_value.borrow(),
    //             &insert_result.data_ticket,
    //             &is_ctx_changed,
    //             &mut self.page_handler
    //         )?;
    //
    //         if is_ctx_changed.get() {
    //             index_ctx.merge_to_meta_doc(meta_doc_mut);
    //             return self.update_current(meta_doc_mut)
    //         }
    //     }
    //
    //     Ok(())
    // }

    // pub fn delete(&mut self, col_name: &str, key: &Value) -> DbResult<Option<Rc<Document>>> {
    //     let (collection_root_pid, meta_doc) = self.find_collection_root_pid_by_name(col_name)?;
    //
    //     let mut delete_wrapper = BTreePageDeleteWrapper::new(
    //         self.page_handler,
    //         collection_root_pid as u32
    //     );
    //     let result = delete_wrapper.delete_item(key)?;
    //
    //     if let Some(deleted_item) = &result {
    //         let index_ctx_opt = IndexCtx::from_meta_doc(meta_doc.borrow());
    //         if let Some(index_ctx) = &index_ctx_opt {
    //             index_ctx.delete_index_by_content(deleted_item.borrow(), self.page_handler)?;
    //         }
    //
    //         return Ok(result)
    //     }
    //
    //     Ok(None)
    // }

    // #[inline]
    // fn index_already_exists(index_doc: &Document, key: &str) -> bool {
    //     match index_doc.get(key) {
    //         Some(_) => true,
    //         _ => false,
    //     }
    // }

    // pub fn create_index(&mut self, col_name: &str, key_name: &str, options: Option<&Document>) -> DbResult<()> {
    //     let (_collection_root_pid, mut meta_doc) = self.find_collection_root_pid_by_name(col_name)?;
    //
    //     match meta_doc.get(meta_doc_key::INDEXES) {
    //         Some(indexes_obj) => match indexes_obj {
    //             Value::Document(index_doc) => {
    //                 if Cursor::index_already_exists(index_doc.borrow(), key_name) {
    //                     return Err(DbErr::IndexAlreadyExists(key_name.into()));
    //                 }
    //
    //                 unimplemented!()
    //             }
    //
    //             _ => {
    //                 panic!("unexpected: indexes object is not a Document");
    //             }
    //
    //         },
    //
    //         None => {
    //             // create indexes
    //             let mut doc = Document::new_without_id();
    //
    //             let root_pid = self.page_handler.alloc_page_id()?;
    //             let options_doc = merge_options_into_default(root_pid, options)?;
    //             doc.insert(key_name.into(), Value::Document(Rc::new(options_doc)));
    //
    //             let mut_meta_doc = Rc::get_mut(&mut meta_doc).unwrap();
    //             mut_meta_doc.insert(meta_doc_key::INDEXES.into(), Value::Document(Rc::new(doc)));
    //
    //             Ok(())
    //         }
    //
    //     }
    // }

    // fn find_collection_root_pid_by_name(&mut self, col_name: &str) -> DbResult<(i64, Rc<Document>)> {
    //     while self.has_next() {
    //         let ticket = self.peek().unwrap();
    //         let doc = self.page_handler.get_doc_from_ticket(&ticket)?;
    //         match doc.get(meta_doc_key::NAME) {
    //             Some(Value::String(name)) => {
    //                 if name.as_ref() == col_name {  // found
    //                     let page_id = doc.get(meta_doc_key::ROOT_PID).unwrap();
    //                     match page_id {
    //                         Value::Int(page_id) => {
    //                             return Ok((*page_id, doc.clone()))
    //                         }
    //
    //                         _ => panic!("page id is not int type")
    //                     }
    //                 }
    //             }
    //
    //             _ => ()
    //         }
    //
    //         let _ = self.next()?;
    //     }
    //
    //     Err(DbErr::CollectionNotFound(col_name.into()))
    // }

    // fn handle_backward_item(&mut self,
    //                         meta_doc_mut: &mut Document,
    //                         left_pid: u32,
    //                         backward_item: &InsertBackwardItem) -> DbResult<()> {
    //
    //     let new_root_id = self.page_handler.alloc_page_id()?;
    //
    //     #[cfg(feature = "log")]
    //     eprintln!("handle backward item, left_pid: {}, new_root_id: {}, right_pid: {}", left_pid, new_root_id, backward_item.right_pid);
    //
    //     let new_root_page = backward_item.write_to_page(self.page_handler, new_root_id, left_pid)?;
    //
    //     meta_doc_mut.insert(meta_doc_key::ROOT_PID.into(), Value::Int(new_root_id as i64));
    //     self.update_current(meta_doc_mut)?;
    //
    //     self.page_handler.pipeline_write_page(&new_root_page)
    // }

}
