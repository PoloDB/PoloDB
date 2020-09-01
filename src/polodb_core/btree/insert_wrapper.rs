use std::rc::Rc;
use crate::DbResult;
use crate::bson::Document;
use crate::page::{RawPage, PageHandler};
use super::btree::{BTreeNode, BTreeNodeDataItem, SearchKeyResult};
use super::wrapper_base::BTreePageWrapperBase;
use crate::error::DbErr;

pub(crate) struct InsertBackwardItem {
    pub content: BTreeNodeDataItem,
    pub right_pid: u32,
}

impl InsertBackwardItem {

    pub(crate) fn write_to_page(&self, new_page_id: u32, left_pid: u32, page_size: u32) -> DbResult<RawPage> {
        let mut result = RawPage::new(new_page_id, page_size);

        let content = vec![self.content.clone()];
        let indexes: Vec<u32> = vec![left_pid, self.right_pid];
        let node = BTreeNode {
            parent_pid: 0,
            pid: new_page_id,
            content,
            indexes
        };

        node.to_raw(&mut result)?;

        Ok(result)
    }

}

// Offset 0:  header(64 bytes)
// Offset 64: Item(500 bytes) * 8
//
// Item struct:
// Offset 0: right pid(4 bytes)
// Offset 4: overflow_pid(4 bytes)
// Offset 8: data
pub struct BTreePageInsertWrapper<'a>(BTreePageWrapperBase<'a>);

impl<'a> BTreePageInsertWrapper<'a> {

    pub(crate) fn new(page_handler: &mut PageHandler, root_page_id: u32) -> BTreePageInsertWrapper {
        let base = BTreePageWrapperBase::new(page_handler, root_page_id);
        BTreePageInsertWrapper(base)
    }

    #[inline]
    pub(crate) fn insert_item(&mut self, doc: Rc<Document>, replace: bool) -> DbResult<Option<InsertBackwardItem>> {
        // insert to root node
        self.insert_item_to_page(self.0.root_page_id, 0, doc, false, replace)
    }

    pub(crate) fn insert_item_to_page(&mut self, pid: u32, parent_pid: u32, doc: Rc<Document>, backward: bool, replace: bool) -> DbResult<Option<InsertBackwardItem>> {
        let mut btree_node: BTreeNode = self.0.get_node(pid, parent_pid)?;

        if btree_node.content.is_empty() {
            btree_node.content.push(BTreeNodeDataItem::with_doc(doc));
            btree_node.indexes.push(0);
            btree_node.indexes.push(0);

            self.0.write_btree_node(&btree_node)?;

            return Ok(None);
        }

        // let mut index: usize = 0;
        let doc_pkey = &doc.pkey_id().expect("primary key not found in document");

        let serach_result = btree_node.search(doc_pkey)?;
        match serach_result {
            SearchKeyResult::Node(index) => {
                return if replace {
                    btree_node.content[index] = BTreeNodeDataItem::with_doc(doc.clone());
                    self.0.write_btree_node(&btree_node)?;

                    Ok(None)
                } else {
                    Err(DbErr::DataExist(doc_pkey.clone()))
                }
            }

            SearchKeyResult::Index(index) => {
                let left_pid = btree_node.indexes[index];
                if backward || left_pid == 0 {  // left is null, insert in current page
                    // insert between index - 1 and index
                    btree_node.content.insert(index, BTreeNodeDataItem::with_doc(doc.clone()));
                    btree_node.indexes.insert(index + 1, 0);  // null page because left_pid is null
                } else {  // left has page
                    // insert to left page
                    let tmp = self.insert_item_to_page(left_pid, pid, doc.clone(), false, replace)?;
                    tmp.map(|backward_item| {
                        btree_node.content.insert(index, backward_item.content);
                        btree_node.indexes.insert(index + 1, backward_item.right_pid);
                    });
                }
            }

        }

        if btree_node.content.len() > (self.0.item_size as usize) {  // need to divide
            return self.divide_and_return_backward(btree_node);
        }

        // write page back
        self.0.write_btree_node(&btree_node)?;

        Ok(None)
    }

    fn divide_and_return_backward(&mut self, btree_node: BTreeNode) -> DbResult<Option<InsertBackwardItem>> {
        let middle_index = btree_node.content.len() / 2;

        // use current page block to store left
        let left = {
            let content = btree_node.content[0..middle_index].to_vec();
            let indexes = btree_node.indexes[0..=middle_index].to_vec();
            BTreeNode {
                parent_pid:  btree_node.parent_pid,
                pid:         btree_node.pid,
                content,
                indexes,
            }
        };

        let right_page_id = self.0.page_handler.alloc_page_id()?;
        // alloc new page to store right
        let right = {
            let content = btree_node.content[(middle_index + 1)..].to_vec();
            let indexes = btree_node.indexes[(middle_index + 1)..].to_vec();
            BTreeNode {
                parent_pid:  btree_node.pid,
                pid:         right_page_id,
                content,
                indexes,
            }
        };

        self.0.write_btree_node(&left)?;
        self.0.write_btree_node(&right)?;

        let middle = &btree_node.content[middle_index];
        Ok(Some(InsertBackwardItem {
            content: middle.clone(),
            right_pid: right_page_id,
        }))
    }

}

