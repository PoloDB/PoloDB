use std::rc::Rc;
use std::collections::LinkedList;
use crate::page::{PageHandler, RawPage};
use crate::btree::{BTreeNode, HEADER_SIZE, ITEM_SIZE, BTreeNodeDataItem, BTreePageWrapper};
use crate::DbResult;
use crate::bson::{Document, Value};
use crate::error::DbErr;

#[derive(Clone)]
struct CursorItem {
    node:         Rc<BTreeNode>,
    index:        usize,
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
    pub fn peek(&self) -> Option<Rc<Document>> {
        if self.btree_stack.is_empty() {
            return None;
        }

        let top = self.btree_stack.back().unwrap();
        Some(top.node.content[top.index].doc.clone())
    }

    pub fn update_current(&mut self, doc: Rc<Document>) -> DbResult<()> {
        let top = self.btree_stack.pop_back().unwrap();

        let new_btree_node: BTreeNode = top.node.clone_with_content(top.index, BTreeNodeDataItem {
            doc,
            overflow_pid: 0,
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
        let result = top.node.content[top.index].doc.clone();

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
        while self.has_next() {
            let doc = self.peek().unwrap();
            match doc.get("name") {
                Some(Value::String(name)) => {
                    if name == col_name {  // found
                        let page_id = doc.get("root_pid").unwrap();
                        match page_id {
                            Value::Int(page_id) => {
                                let mut btree_wrapper = BTreePageWrapper::new(self.page_handler, *page_id as u32);

                                let backward = btree_wrapper.insert_item(doc_value.clone(), false)?;
                                match backward {
                                    Some(backward_item) => {
                                        let new_root_id = self.page_handler.alloc_page_id()?;
                                        let new_root_page = backward_item.write_to_page(new_root_id, *page_id as u32, self.page_handler.page_size)?;

                                        let mut new_doc = doc.clone();
                                        let doc = Rc::make_mut(&mut new_doc);
                                        doc.insert("page_id".into(), Value::Int(new_root_id as i64));
                                        self.update_current(new_doc)?;

                                        self.page_handler.pipeline_write_page(&new_root_page)?
                                    },
                                    None => ()
                                }

                                return Ok(())
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

}
