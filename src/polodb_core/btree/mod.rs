
mod wrapper_base;
mod insert_wrapper;
mod delete_wrapper;
pub mod counter_helper;
pub(crate) mod delete_all_helper;
mod vli;

pub(crate) use delete_wrapper::BTreePageDeleteWrapper;
pub(crate) use insert_wrapper::{BTreePageInsertWrapper, InsertBackwardItem, InsertResult};

use std::cmp::Ordering;
use bson::Bson;
use bson::oid::ObjectId;
use bson::spec::ElementType;
use crate::db::DbResult;
use crate::page::{RawPage, PageType};
use crate::error::{DbErr, mk_unexpected_header_for_btree_page};
use crate::data_ticket::DataTicket;
use crate::session::Session;

pub const HEADER_SIZE: u32      = 64;

// | right_pid | key_ty_int | key content | ticket  |
// | 4 bytes   | 2 bytes    | 12 bytes    | 6 bytes |
pub const ITEM_SIZE: u32        = 24;

const BTREE_ENTRY_KEY_CONTENT_SIZE: usize = 12;

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

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.content.is_empty()
    }

    // binary search the content
    // find the content or index
    pub(crate) fn search(&self, key: &Bson) -> DbResult<SearchKeyResult> {
        let mut low: i32 = 0;
        let mut high: i32 = (self.content.len() - 1) as i32;

        while low <= high {
            let middle = (low + high) / 2;
            let target_key = &self.content[middle as usize].key;

            let cmp_result = crate::bson_utils::value_cmp(key, target_key)?;

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

    #[inline]
    pub fn clone_with_contents(&self, content: Vec<BTreeNodeDataItem>, indexes: Vec<u32>) -> BTreeNode {
        BTreeNode {
            parent_pid: self.parent_pid,
            pid: self.pid,
            content,
            indexes,
        }
    }

    fn from_raw_lossy(page: &RawPage, parent_pid: u32, item_size: u32, page_handler: &mut dyn Session) -> DbResult<BTreeNode> {
        debug_assert_ne!(page.page_id, 0, "page id is zero, parent pid: {}", parent_pid);

        let first_left_pid = page.get_u32(4);
        let mut content = vec![];
        let mut indexes = vec![ first_left_pid ];

        let len = page.get_u16(2);

        if (len as u32) > item_size {  // data error
            return Err(DbErr::ItemSizeGreaterThanExpected);
        }

        for i in 0..len {
            let offset: u32 = HEADER_SIZE + (i as u32) * ITEM_SIZE;

            let right_pid = page.get_u32(offset);

            let node_data_item = BTreeNode::parse_node_data_item(&page, offset, page_handler)?;

            content.push(node_data_item);
            indexes.push(right_pid);
        }

        Ok(BTreeNode {
            pid: page.page_id,
            parent_pid,
            content,
            indexes,
        })
    }

    // Offset 0: magic(2 bytes)
    // Offset 2: items_len(2 bytes)
    // Offset 4: left_pid (4 bytes)
    // Offset 8: next_pid (4 bytes)
    pub(crate) fn from_raw(page: &RawPage, parent_pid: u32, item_size: u32, page_handler: &mut dyn Session) -> DbResult<BTreeNode> {
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
            let err = mk_unexpected_header_for_btree_page(page.page_id, &magic, &page.data[0..2]);
            return Err(err)
        }

        BTreeNode::from_raw_lossy(page, parent_pid, item_size, page_handler)
    }

    fn parse_node_data_item(page: &RawPage, begin_offset: u32, page_handler: &mut dyn Session) -> DbResult<BTreeNodeDataItem> {
        let is_complex = page.get_u8(begin_offset + 4);
        if is_complex != 0 {
            return BTreeNode::parse_complex_data_item(page, begin_offset, page_handler);
        }

        let key_ty_int = page.get_u8(begin_offset + 4 + 1);  // use to parse data

        let element_type = ElementType::from(key_ty_int);
        let key: Bson = match element_type {
            Some(ElementType::ObjectId) => {
                let oid_bytes_begin = (begin_offset + 6) as usize;
                let mut oid_bytes = [0; 12];
                oid_bytes.copy_from_slice(&page.data[oid_bytes_begin..(oid_bytes_begin + 12)]);
                let oid = ObjectId::from(oid_bytes);

                oid.into()
            }

            Some(ElementType::Boolean) => {
                let value_begin_offset = (begin_offset + 6) as usize;
                let value = page.data[value_begin_offset];

                let bl_value = value != 0;

                Bson::Boolean(bl_value)
            }

            Some(ElementType::Int32) => {
                let value_begin_offset = (begin_offset + 6) as usize;

                let (int_value, _) = vli::decode_u64(&page.data[value_begin_offset..])?;

                Bson::Int32(int_value as i32)
            }

            Some(ElementType::Int64) => {
                let value_begin_offset = (begin_offset + 6) as usize;

                let (int_value, _) = vli::decode_u64(&page.data[value_begin_offset..])?;

                Bson::Int64(int_value as i64)
            }

            Some(ElementType::String) => {
                let value_begin_offset = (begin_offset + 6) as usize;

                let mut buffer = Vec::new();

                let mut offset = 0;
                loop {
                    if offset >= BTREE_ENTRY_KEY_CONTENT_SIZE {
                        break;
                    }

                    let ch = page.data[value_begin_offset + offset];

                    if ch == 0 {
                        break;
                    }

                    buffer.push(ch);

                    offset += 1;
                }

                let str = unsafe {
                    String::from_utf8_unchecked(buffer)
                };

                str.into()
            }

            _ => {
                let error_msg = format!("type {} is not suitable for _id", key_ty_int);
                return Err(DbErr::ParseError(error_msg));
            },

        };

        let data_ticket = BTreeNode::parse_data_item_ticket(page, begin_offset);

        Ok(BTreeNodeDataItem {
            key,
            data_ticket,
        })
    }

    fn parse_complex_data_item(page: &RawPage, begin_offset: u32, page_handler: &mut dyn Session) -> DbResult<BTreeNodeDataItem> {
        let data_ticket = BTreeNode::parse_data_item_ticket(page, begin_offset);
        let doc = page_handler.get_doc_from_ticket(&data_ticket)?.unwrap();
        let pkey = doc.get("_id").unwrap().into();
        Ok(BTreeNodeDataItem {
            key: pkey,
            data_ticket,
        })
    }

    #[inline]
    fn parse_data_item_ticket(page: &RawPage, begin_offset: u32) -> DataTicket {
        let ticket_bytes = (begin_offset + 6 + 12) as usize;
        let ticket_bytes = &page.data[ticket_bytes..(ticket_bytes + 6)];
        DataTicket::from_bytes(ticket_bytes)
    }

    pub(crate) fn to_raw(&self, page: &mut RawPage) -> DbResult<()> {
        let items_len = self.content.len() as u16;

        let page_type = PageType::BTreeNode;
        let magic = page_type.to_magic();
        page.seek(0);
        page.put(&magic);

        page.seek(2);
        page.put_u16(items_len);

        if let Some(_first) = self.content.first() {
            page.seek(4);

            let left_id = self.indexes.first().expect("get first left id failed");
            page.put_u32(*left_id);
        };

        let mut index = 0;
        while index < self.content.len() {
            let item = &self.content[index];
            let right_pid = self.indexes[index + 1];

            let offset: u32 = HEADER_SIZE + (index as u32) * ITEM_SIZE;

            page.seek(offset);

            // 4 bytes for right_pid
            page.put_u32(right_pid);

            // put entry key
            BTreeNode::entry_key_to_bytes(page, &item.key)?;

            // 6 bytes for ticket
            let ticket_bytes = item.data_ticket.to_bytes();
            page.put(&ticket_bytes);

            index += 1;
        }

        Ok(())
    }

    // | flag   | key_type | key_content |
    // | 1 byte | 1 byte   | 12 bytes    |
    //
    // if the length of key_content is greater than 12
    // then the flag should be 1, and the key_content should be zero
    fn entry_key_to_bytes(page: &mut RawPage, key: &Bson) -> DbResult<()> {
        match key {
            Bson::ObjectId(oid) => {
                BTreeNode::put_standard_content_key(page, key);

                // 12 bytes for key content
                let bytes = oid.bytes();

                page.put(&bytes);

                Ok(())
            }

            Bson::Boolean(bl) => {
                BTreeNode::put_standard_content_key(page, key);

                let mut buffer: [u8; BTREE_ENTRY_KEY_CONTENT_SIZE] = [0; BTREE_ENTRY_KEY_CONTENT_SIZE];
                buffer[0] = if *bl {
                    1
                } else {
                    0
                };

                page.put(&buffer);

                Ok(())
            }

            Bson::Int32(int) => {
                BTreeNode::put_standard_content_key(page, key);

                let mut buffer = Vec::with_capacity(BTREE_ENTRY_KEY_CONTENT_SIZE);
                vli::encode(&mut buffer, *int as i64)?;

                buffer.resize(BTREE_ENTRY_KEY_CONTENT_SIZE, 0);

                page.put(&buffer);

                Ok(())
            }

            Bson::Int64(int) => {
                BTreeNode::put_standard_content_key(page, key);

                let mut buffer = Vec::with_capacity(BTREE_ENTRY_KEY_CONTENT_SIZE);
                vli::encode(&mut buffer, *int)?;

                buffer.resize(BTREE_ENTRY_KEY_CONTENT_SIZE, 0);

                page.put(&buffer);

                Ok(())
            }

            Bson::String(str) => {
                let str_len = str.len();

                if str_len > BTREE_ENTRY_KEY_CONTENT_SIZE {
                    return BTreeNode::put_string_complex_key(page, key);
                }

                BTreeNode::put_standard_content_key(page, key);
                let mut buffer: [u8; BTREE_ENTRY_KEY_CONTENT_SIZE] = [0; BTREE_ENTRY_KEY_CONTENT_SIZE];

                buffer[0..str_len].copy_from_slice(str.as_bytes());

                page.put(&buffer);

                Ok(())
            }

            _ => {
                let name = format!("{:?}", key);
                Err(DbErr::NotAValidKeyType(name))
            }
        }
    }

    // | 1      | ty_int |
    // | 1 byte | 1 byte |
    fn put_string_complex_key(page: &mut RawPage, key: &Bson) -> DbResult<()> {
        let key_ty_int = key.element_type() as u8;

        page.put_u8(1);
        page.put_u8(key_ty_int);

        let buffer: [u8; 12] = [0; 12];
        page.put(&buffer);

        Ok(())
    }

    // | 0      | ty_int |
    // | 1 byte | 1 byte |
    fn put_standard_content_key(page: &mut RawPage, key: &Bson) {
        let key_ty_int = key.element_type() as u8;

        // 2 bytes for key ty_int
        page.put_u8(0);
        page.put_u8(key_ty_int);
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
    pub(crate) key:          Bson,
    pub(crate) data_ticket:  DataTicket,
}
