use std::sync::{Arc, Mutex};
use std::collections::LinkedList;
use bson::{Document, Bson};
use crate::btree::{BTreePageDelegate, BTreePageDelegateWithKey, SearchKeyResult};
use crate::DbResult;
use crate::data_ticket::DataTicket;
use crate::session::Session;

#[derive(Clone)]
struct CursorItem {
    node:         Arc<Mutex<BTreePageDelegateWithKey>>,
    index:        usize,  // pointer point to the current node
}

impl CursorItem {

    fn new(node: BTreePageDelegateWithKey, index: usize) -> CursorItem {
        CursorItem {
            node: Arc::new(Mutex::new(node)),
            index,
        }
    }

    fn done(&self) -> bool {
        let node_inner = self.node.lock().unwrap();
        self.index >= node_inner.len()
    }

    fn right_pid(&self) -> u32 {
        let node_inner = self.node.lock().unwrap();
        node_inner.right_pid
    }
}

pub(crate) struct Cursor {
    root_pid:           u32,
    btree_stack:        LinkedList<CursorItem>,
    current:            Option<Document>,
}

impl Cursor {

    pub fn new(root_pid: u32) -> Cursor {
        Cursor {
            root_pid,
            btree_stack: LinkedList::new(),
            current: None,
        }
    }

    pub fn reset(&mut self, page_handler: &dyn Session) -> DbResult<()> {
        self.mk_initial_btree(page_handler, self.root_pid)?;

        if self.btree_stack.is_empty() {
            return Ok(());
        }

        self.push_all_left_nodes(page_handler)?;

        Ok(())
    }

    pub fn reset_by_pkey(&mut self, session: &dyn Session, pkey: &Bson) -> DbResult<bool> {
        self.btree_stack.clear();

        let mut current_pid = self.root_pid;

        // recursively find the item
        while current_pid > 0 {
            let btree_page = session.read_page(current_pid)?;
            let delegate = BTreePageDelegate::from_page(btree_page.as_ref(), 0)?;
            let btree_node = BTreePageDelegateWithKey::read_from_session(delegate, session)?;

            if btree_node.is_empty() {
                return Ok(false);
            }

            let search_result = btree_node.search(pkey)?;
            match search_result {
                SearchKeyResult::Node(index) => {
                    self.btree_stack.push_back(CursorItem::new(btree_node, index));
                    return Ok(true)
                }

                SearchKeyResult::Index(index) => {
                    let next_pid = btree_node.get_left_pid(index);
                    if next_pid == 0 {
                        return Ok(false);
                    }

                    self.btree_stack.push_back(CursorItem::new(btree_node, index));

                    current_pid = next_pid;
                }

            }
        }

        Ok(false)
    }

    fn mk_initial_btree(&mut self, session: &dyn Session, root_page_id: u32) -> DbResult<()> {
        self.btree_stack.clear();

        let btree_page = session.read_page(root_page_id)?;
        let delegate = BTreePageDelegate::from_page(btree_page.as_ref(), 0)?;
        let btree_node = BTreePageDelegateWithKey::read_from_session(delegate, session)?;

        if !btree_node.is_empty() {
            self.btree_stack.push_back(CursorItem::new(btree_node, 0));
        }

        Ok(())
    }

    fn push_all_left_nodes(&mut self, session: &dyn Session) -> DbResult<()> {
        if self.btree_stack.is_empty() {
            return Ok(());
        }
        let mut top = self.btree_stack.back().unwrap().clone();
        let pid = {
            let btree_node = top.node.lock().unwrap();
            btree_node.page_id()
        };
        let mut left_pid = {
            let btree_node = top.node.lock().unwrap();
            btree_node.get_left_pid(top.index)
        };

        while left_pid != 0 {
            let btree_page = session.read_page(left_pid)?;
            let delegate = BTreePageDelegate::from_page(btree_page.as_ref(), pid)?;
            let btree_node = BTreePageDelegateWithKey::read_from_session(delegate, session)?;

            self.btree_stack.push_back(CursorItem::new(btree_node, 0));

            top = self.btree_stack.back().unwrap().clone();
            let top_content = top.node.lock()?;
            left_pid = top_content.get_left_pid(top.index);
        }

        Ok(())
    }

    pub fn peek(&mut self) -> Option<DataTicket> {
        if self.btree_stack.is_empty() {
            return None;
        }

        let top = self.btree_stack.back().unwrap();
        let top_content = top.node.lock().unwrap();

        if top_content.is_empty() {
            panic!("AAA");
        }
        assert!(!top_content.is_empty(), "top node content is empty, page_id: {}", top_content.page_id());

        let ticket = top_content.get_item(top.index).payload.clone();
        Some(ticket)
    }

    pub fn update_current(&mut self, session: &dyn Session, doc: &Document) -> DbResult<()> {
        let top = self.btree_stack.pop_back().unwrap();

        {
            let mut content = top.node.lock()?;

            session.free_data_ticket(&content.get_item(top.index).payload)?;
            let new_ticket = session.store_doc(doc)?;
            content.update_payload(top.index, new_ticket);
        }

        self.btree_stack.push_back(top);

        self.sync_top_btree_node(session)
    }

    fn sync_top_btree_node(&mut self, session: &dyn Session) -> DbResult<()> {
        let top = self.btree_stack.back().unwrap();
        let top_content = top.node.lock()?;

        let page = top_content.generate_page()?;

        session.write_page(&page)
    }

    #[inline]
    pub fn has_next(&self) -> bool {
        !self.btree_stack.is_empty()
    }

    pub fn next(&mut self, session: &dyn Session) -> DbResult<Option<Document>> {
        if self.btree_stack.is_empty() {
            return Ok(None);
        }

        let top = self.btree_stack.pop_back().unwrap();
        let (result, top_content_len) = {
            let top_content = top.node.lock()?;
            let result_ticket = &top_content.get_item(top.index).payload;
            let result = session.get_doc_from_ticket(result_ticket)?;
            (result, top_content.len())
        };

        let next_index = top.index + 1;

        if next_index >= top_content_len {  // right most index
            let right_most_index = top.right_pid();

            if right_most_index != 0 {
                self.btree_stack.push_back(CursorItem {
                    node: top.node.clone(),
                    index: next_index,
                });

                self.push_all_left_nodes(session)?;

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

        self.push_all_left_nodes(session)?;

        self.current = Some(result.clone());
        Ok(Some(result))
    }

    pub fn pop_all_right_most_item(&mut self) {
        if self.btree_stack.is_empty() {
            return;
        }

        let mut top = self.btree_stack.back().unwrap();

        while top.done() {
            self.btree_stack.pop_back();

            if self.btree_stack.is_empty() {
                return;
            }
            top = self.btree_stack.back().unwrap();
        }
    }

}
