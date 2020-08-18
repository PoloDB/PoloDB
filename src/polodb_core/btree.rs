use std::cmp::Ordering;
use std::rc::Rc;

use crate::db::DbResult;
use crate::page::{RawPage, PageHandler, PageType};
use crate::error::DbErr;
use crate::bson::Document;

pub(crate) static HEADER_SIZE: u32      = 64;
pub(crate) static ITEM_SIZE: u32        = 500;
pub(crate) static ITEM_HEADER_SIZE: u32 = 12;

pub(crate) struct BTreeNode {
    pub parent_pid:  u32,
    pub pid:         u32,
    pub content:     Vec<BTreeNodeDataItem>,
    pub indexes:     Vec<u32>,
}

impl BTreeNode {

    pub fn clone_with_content(&self, new_index: usize, new_item: BTreeNodeDataItem) -> BTreeNode {
        let mut content: Vec<BTreeNodeDataItem> = Vec::with_capacity(self.content.capacity());

        for (index, item) in self.content.iter().enumerate() {
            if index == new_index {
                content.push(new_item.clone());
            } else {
                content.push(item.clone());
            }
        }

        BTreeNode {
            parent_pid: self.parent_pid,
            pid: self.pid,
            content,
            indexes: self.indexes.clone(),
        }
    }

    // Offset 0: magic(2 bytes)
    // Offset 2: items_len(2 bytes)
    // Offset 4: left_pid (4 bytes)
    // Offset 8: next_pid (4 bytes)
    pub fn from_raw(page: &RawPage, parent_pid: u32, item_size: u32) -> DbResult<BTreeNode> {
        let page_type = PageType::BTreeNode;
        let magic = page_type.to_magic();
        if page.data[0..2] != magic {
            if page.data[0..2] == [0, 0] {  // null page
                return Ok(BTreeNode {
                    pid: page.page_id,
                    parent_pid,
                    content: vec![],
                    indexes: vec![ 0 ],
                });
            }
            return Err(DbErr::ParseError);
        }

        let mut left_pid = page.get_u32(4);
        let mut content = vec![];
        let mut indexes = vec![ left_pid ];

        let len = page.get_u16(2);

        if (len as u32) > item_size {  // data error
            return Err(DbErr::ItemSizeGreaterThenExpected);
        }

        for i in 0..len {
            let offset: u32 = HEADER_SIZE + (i as u32) * ITEM_SIZE;

            let right_pid = page.get_u32(offset);

            let overflow_pid = page.get_u32(4);  // use to parse data

            let data_offset: usize = (offset + ITEM_HEADER_SIZE) as usize;

            let data = page.data[data_offset..(data_offset + ((ITEM_SIZE - ITEM_HEADER_SIZE) as usize))].to_vec();
            let doc = Rc::new(Document::from_bytes(&data)?);

            content.push(BTreeNodeDataItem { doc, overflow_pid });

            indexes.push(right_pid);

            left_pid = right_pid;
        }

        Ok(BTreeNode {
            pid: page.page_id,
            parent_pid,
            content,
            indexes,
        })
    }

    pub fn to_raw(&self, page: &mut RawPage) -> DbResult<()> {
        let items_len = self.content.len() as u16;

        let page_type = PageType::BTreeNode;
        let magic = page_type.to_magic();
        page.seek(0);
        page.put(&magic);

        page.seek(2);
        page.put_u16(items_len);

        self.content.first().map(|first| {
            page.seek(4);

            let left_id = self.indexes.first().expect("get first left id failed");
            page.put_u32(*left_id);
        });

        let mut index = 0;
        while index < self.content.len() {
            let item = &self.content[index];
            let right_pid = self.indexes[index + 1];

            let offset: u32 = HEADER_SIZE + (index as u32) * ITEM_SIZE;

            page.seek(offset);
            page.put_u32(right_pid);

            // TODO: overflow pid
            page.put_u64(0);

            // TODO: write data
            let doc_bytes = item.doc.to_bytes()?;
            page.put(&doc_bytes);

            index += 1;
        }

        Ok(())
    }

    fn is_root(&self) -> bool {
        self.parent_pid == 0
    }

}

#[derive(Clone)]
pub(crate) struct BTreeNodeDataItem {
    pub doc:          Rc<Document>,
    pub overflow_pid: u32,
}

impl BTreeNodeDataItem {

    fn with_doc(doc: Rc<Document>) -> BTreeNodeDataItem {
        BTreeNodeDataItem {
            doc,
            overflow_pid: 0,
        }
    }

}

pub(crate) struct BackwardItem {
    content: BTreeNodeDataItem,
    right_pid: u32,
}

impl BackwardItem {

    pub fn write_to_page(&self, new_page_id: u32, left_pid: u32, page_size: u32) -> DbResult<RawPage> {
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
pub(crate) struct BTreePageWrapper<'a> {
    page_handler:       &'a mut PageHandler,
    root_page_id:       u32,
    item_size:          u32,
}

impl<'a> BTreePageWrapper<'a> {

    pub fn new(page_handler: &mut PageHandler, root_page_id: u32) -> BTreePageWrapper {
        let item_size = (page_handler.page_size - HEADER_SIZE) / ITEM_SIZE;

        BTreePageWrapper {
            page_handler,
            root_page_id, item_size
        }
    }

    pub fn insert_item(&mut self, doc: Rc<Document>, replace: bool) -> DbResult<Option<BackwardItem>> {
        // insert to root node
        self.insert_item_to_page(self.root_page_id, 0, doc, false, replace)
    }

    fn get_node(&mut self, pid: u32, parent_pid: u32) -> DbResult<BTreeNode> {
        let raw_page = self.page_handler.pipeline_read_page(pid)?;

        BTreeNode::from_raw(&raw_page, parent_pid, self.item_size)
    }

    pub fn insert_item_to_page(&mut self, pid: u32, parent_pid: u32, doc: Rc<Document>, backward: bool, replace: bool) -> DbResult<Option<BackwardItem>> {
        let mut btree_node: BTreeNode = self.get_node(pid, parent_pid)?;

        if btree_node.content.is_empty() {
            btree_node.content.push(BTreeNodeDataItem::with_doc(doc));
            btree_node.indexes.push(0);
            btree_node.indexes.push(0);

            self.write_btree_node(&btree_node)?;

            return Ok(None);
        }

        let mut index: usize = 0;
        let doc_pkey = doc.pkey_id().expect("primary key not found in document");

        while index < btree_node.content.len() {
            let target = &btree_node.content[index];
            let target_key = target.doc.pkey_id().expect("primary key not found in target document");
            let left_pid = btree_node.indexes[index];

            let cmp_result = doc_pkey.value_cmp(&target_key)?;

            match cmp_result {
                Ordering::Equal => {
                    return if replace {
                        btree_node.content[index] = BTreeNodeDataItem::with_doc(doc.clone());
                        self.write_btree_node(&btree_node)?;

                        Ok(None)
                    } else {
                        Err(DbErr::DataExist(doc_pkey))
                    }
                }

                Ordering::Less => {
                    if backward || left_pid == 0 {  // left is null, insert in current page
                        // insert between index - 1 and index
                        btree_node.content.insert(index, BTreeNodeDataItem::with_doc(doc.clone()));
                        btree_node.indexes.insert(index + 1, 0);  // null page because left_pid is null
                        break;
                    } else {  // left has page
                        // insert to left page
                        let tmp = self.insert_item_to_page(left_pid, pid, doc.clone(), false, replace)?;
                        tmp.map(|backward_item| {
                            btree_node.content.insert(index, backward_item.content);
                            btree_node.indexes.insert(index + 1, backward_item.right_pid);
                        });
                    }
                    break;  // finish loop
                }

                Ordering::Greater => () // next iter
            }

            index += 1;
        }

        if index >= btree_node.content.len() - 1 {  // greater than the last
            let right_pid = btree_node.indexes[index];  // index is already equal content.len()
            if backward || right_pid == 0 {  // right page is null, insert in current page
                btree_node.content.push(BTreeNodeDataItem::with_doc(doc.clone()));
                btree_node.indexes.push(0);
            } else {  // insert to right page
                let tmp = self.insert_item_to_page(right_pid, pid, doc, false, replace)?;
                tmp.map(|backward_item| {
                    btree_node.content.push(backward_item.content);
                    btree_node.indexes.push(0);
                });
            }
        }

        if btree_node.content.len() > (self.item_size as usize) {  // need to divide
            return self.divide_and_return_backward(btree_node);
        }

        // write page back
        self.write_btree_node(&btree_node)?;

        Ok(None)
    }

    fn write_btree_node(&mut self, node: &BTreeNode) -> DbResult<()> {
        let mut raw_page = RawPage::new(node.pid, self.page_handler.page_size);

        node.to_raw(&mut raw_page)?;

        self.page_handler.pipeline_write_page(&raw_page)
    }

    fn divide_and_return_backward(&mut self, btree_node: BTreeNode) -> DbResult<Option<BackwardItem>> {
        let middle_index = (btree_node.content.len() + 1) / 2;

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

        let right_page_id = self.page_handler.alloc_page_id()?;
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

        self.write_btree_node(&left)?;
        self.write_btree_node(&right)?;

        let middle = &btree_node.content[middle_index];
        Ok(Some(BackwardItem {
            content: middle.clone(),
            right_pid: right_page_id,
        }))
    }

}
