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

}
