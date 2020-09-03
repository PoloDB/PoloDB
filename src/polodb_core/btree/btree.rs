use std::cmp::Ordering;
use std::rc::Rc;

use crate::db::DbResult;
use crate::page::{RawPage, PageType, PageHandler, OverflowPageWrapper};
use crate::error::{DbErr, parse_error_reason};
use crate::bson::{Document, Value};

pub static HEADER_SIZE: u32      = 64;
pub static ITEM_SIZE: u32        = 500;
pub static ITEM_HEADER_SIZE: u32 = 12;

pub enum SearchKeyResult {
    Node(usize),
    Index(usize),
}

#[derive(Clone)]
pub struct BTreeNode {
    pub parent_pid:  u32,
    pub pid:         u32,
    pub content:     Vec<BTreeNodeDataItem>,
    pub indexes:     Vec<u32>,
}

impl BTreeNode {

    #[inline]
    pub(crate) fn is_leaf(&self) -> bool {
        self.indexes[0] == 0
    }

    // binary search the content
    // find the content or index
    pub(crate) fn search(&self, key: &Value) -> DbResult<SearchKeyResult> {
        let mut low: i32 = 0;
        let mut high: i32 = (self.content.len() - 1) as i32;

        while low <= high {
            let middle = (low + high) / 2;
            let target_key = &self.content[middle as usize].doc.pkey_id().expect("primary key not found in target document");

            let cmp_result = key.value_cmp(target_key)?;

            match cmp_result {
                Ordering::Equal =>
                    return Ok(SearchKeyResult::Node(middle as usize)),

                Ordering::Less => {
                    high = middle - 1;
                }

                Ordering::Greater => {
                    low = middle + 1;
                }

            }
        }

        Ok(SearchKeyResult::Index(std::cmp::max(low, high) as usize))
    }

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
    pub(crate) fn from_raw(page_handler: &mut PageHandler, page: &RawPage, parent_pid: u32, item_size: u32) -> DbResult<BTreeNode> {
        #[cfg(debug_assertions)]
        if page.page_id == 0 {
            panic!("page id is zero, parent pid: {}", parent_pid);
        }

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
            return Err(DbErr::ParseError(parse_error_reason::UNEXPECTED_HEADER_FOR_BTREE_PAGE.into()));
        }

        let first_left_pid = page.get_u32(4);
        let mut content = vec![];
        let mut indexes = vec![ first_left_pid ];

        let len = page.get_u16(2);

        if (len as u32) > item_size {  // data error
            return Err(DbErr::ItemSizeGreaterThenExpected);
        }

        for i in 0..len {
            let offset: u32 = HEADER_SIZE + (i as u32) * ITEM_SIZE;

            let right_pid = page.get_u32(offset);

            let overflow_pid = page.get_u32(offset + 4);  // use to parse data

            let data_offset = (offset + ITEM_HEADER_SIZE) as usize;

            let item_content_size = (ITEM_SIZE - ITEM_HEADER_SIZE) as usize;

            let mut buffer: Vec<u8> = Vec::with_capacity(0);
            let data: &[u8] = if overflow_pid == 0 {
                &page.data[data_offset..(data_offset + item_content_size)]
            } else {
                buffer.reserve(4096);
                OverflowPageWrapper::recursively_get_overflow_data(page_handler, &mut buffer, overflow_pid)?;
                &buffer
            };

            let doc = Rc::new(Document::from_bytes(data)?);

            content.push(BTreeNodeDataItem { doc, overflow_pid });

            indexes.push(right_pid);
        }

        Ok(BTreeNode {
            pid: page.page_id,
            parent_pid,
            content,
            indexes,
        })
    }

    pub(crate) fn to_raw(&self, page_handler: &mut PageHandler, page: &mut RawPage) -> DbResult<()> {
        let items_len = self.content.len() as u16;

        let page_type = PageType::BTreeNode;
        let magic = page_type.to_magic();
        page.seek(0);
        page.put(&magic);

        page.seek(2);
        page.put_u16(items_len);

        self.content.first().map(|_first| {
            page.seek(4);

            let left_id = self.indexes.first().expect("get first left id failed");
            page.put_u32(*left_id);
        });

        let mut index = 0;
        while index < self.content.len() {
            let item = &self.content[index];
            let right_pid = self.indexes[index + 1];

            let offset: u32 = HEADER_SIZE + (index as u32) * ITEM_SIZE;

            let doc_bytes = item.doc.to_bytes()?;
            let item_content_size = (ITEM_SIZE - ITEM_HEADER_SIZE) as usize;

            let (current_page_content, overflow_pid) = OverflowPageWrapper::handle_overflow(page_handler, &doc_bytes, item_content_size)?;

            page.seek(offset);
            page.put_u32(right_pid);

            page.put_u32(overflow_pid);
            page.put_u32(0);  // reserve

            page.put(current_page_content);

            index += 1;
        }

        Ok(())
    }

    #[allow(dead_code)]
    fn is_root(&self) -> bool {
        self.parent_pid == 0
    }

    pub(crate) fn shift_head(&mut self) -> (u32, BTreeNodeDataItem) {
        if self.content.is_empty() {
            panic!("btree content is empty, pid: {}", self.pid);
        }

        let first_index = self.indexes[0];
        let first_content: BTreeNodeDataItem = self.content[0].clone();

        self.indexes.remove(0);
        self.content.remove(0);

        (first_index, first_content)
    }

    pub(crate) fn shift_last(&mut self) -> (BTreeNodeDataItem, u32) {
        if self.content.is_empty() {
            panic!("btree content is empty, pid: {}", self.pid);
        }

        let last_index = self.indexes[self.indexes.len() - 1];
        let last_content = self.content[self.content.len() - 1].clone();

        self.indexes.remove(self.indexes.len() - 1);
        self.content.remove(self.content.len() - 1);

        (last_content, last_index)
    }

    #[inline]
    pub(crate) fn insert_head(&mut self, idx: u32, item: BTreeNodeDataItem) {
        self.indexes.insert(0, idx);
        self.content.insert(0, item);
    }

    #[inline]
    pub(crate) fn insert_back(&mut self, item: BTreeNodeDataItem, idx: u32) {
        self.indexes.push(idx);
        self.content.push(item);
    }

}

#[derive(Clone)]
pub struct BTreeNodeDataItem {
    pub doc:          Rc<Document>,
    pub overflow_pid: u32,
}

impl BTreeNodeDataItem {

    pub(crate) fn with_doc(doc: Rc<Document>) -> BTreeNodeDataItem {
        BTreeNodeDataItem {
            doc,
            overflow_pid: 0,
        }
    }

}
