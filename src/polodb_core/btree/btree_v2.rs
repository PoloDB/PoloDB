use std::cmp::Ordering;
use std::io::{Read, Write};
use std::num::NonZeroU32;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bson::Bson;
use bson::oid::ObjectId;
use bson::spec::ElementType;
use crate::btree::{SearchKeyResult, vli};
use crate::data_ticket::DataTicket;
use crate::{DbErr, DbResult};
use crate::page::{PageType, RawPage};
use crate::session::Session;

/// | left pid | key type | key size | key content | payload |
/// | 4 bytes  | 1 byte   | 1 byte   | 0-254 bytes | 6 bytes |
///
/// If the key size is 255, that's saying
/// the length of the key >= 255.
/// The key will be stored in the data storage.
/// The key content field will be the data ticket.
///
/// The max length of the [`BTreeDataItem`] will be 270 bytes
pub(crate) struct BTreeDataItem {
    pub left_pid:    u32,
    pub key_ty:      u8,
    pub key_len:     u8,  // 255 for data ticket
    pub key_content: Vec<u8>,
    pub payload:     DataTicket,
}

impl BTreeDataItem {

    const MAX_SIZE: usize = 266;

    pub fn buffer_size(&self) -> u32 {
        let mut result = 4 + 1 + 1;

        if self.key_len == 255 {  // key is stored in data storage
            result += 6;
        } else {
            result += self.key_len as u32;
        }

        result += 6;

        result
    }

    pub fn from_bytes(mut bytes: &[u8]) -> DbResult<BTreeDataItem> {
        let left_pid = bytes.read_u32::<BigEndian>()?;
        let key_ty = bytes.read_u8()?;
        let key_len = bytes.read_u8()?;
        let key_content = if key_len == 255 {
            let mut data: Vec<u8> = vec![0; 6];
            bytes.read_exact(&mut data)?;
            data
        } else {
            let mut data: Vec<u8> = vec![0; key_len as usize];
            bytes.read_exact(&mut data)?;
            data
        };
        let payload = DataTicket::from_bytes(bytes);
        Ok(BTreeDataItem {
            left_pid,
            key_ty,
            key_len,
            key_content,
            payload,
        })
    }

    pub fn write_bytes<W: Write>(&self, w: &mut W) -> DbResult<()> {
        w.write_u32::<BigEndian>(self.left_pid)?;
        w.write_u8(self.key_ty)?;
        w.write_u8(self.key_len)?;
        w.write(&self.key_content)?;
        let payload = self.payload.to_bytes();
        w.write(&payload)?;

        Ok(())
    }

}

/// | magic | item size | remain size | right pid | preserved |
/// | 2b    | 2 bytes   | 2 bytes     | 4 bytes   | 6 bytes   |
///
/// Header size: 16
pub(crate) struct BTreePageDelegate {
    page_id:       u32,
    parent_id:     u32,
    page_size:     NonZeroU32,
    remain_size:   i32,  // can be negative
    pub right_pid: u32,
    content:       Vec<BTreeDataItem>,
}

impl BTreePageDelegate {

    const HEADER_SIZE: usize = 16;

    pub fn from_page(raw_page: &RawPage, parent_id: u32) -> DbResult<BTreePageDelegate> {
        let page_size = NonZeroU32::new(raw_page.data.len() as u32).unwrap();
        let item_size = raw_page.get_u16(2);
        let remain_size = raw_page.get_u16(4) as i32;
        let right_pid = raw_page.get_u32(6);

        let mut bottom_bar = page_size.get() as u16;

        let mut content = Vec::with_capacity(item_size as usize);

        for i in 0..item_size {
            let offset = (BTreePageDelegate::HEADER_SIZE + (i * 2) as usize) as u16;

            let data = BTreeDataItem::from_bytes(&raw_page.data[(offset as usize)..(bottom_bar as usize)])?;
            content.push(data);

            bottom_bar = offset;
        }

        Ok(BTreePageDelegate {
            page_id: raw_page.page_id,
            parent_id,
            page_size,
            remain_size,
            right_pid,
            content,
        })
    }

    pub fn generate_page(&self) -> DbResult<RawPage> {
        let mut result = RawPage::new(self.page_id, self.page_size);

        let page_type = PageType::BTreeNode;
        result.put(&page_type.to_magic());

        // item size
        result.put_u16(self.content.len() as u16);

        // remain size
        assert!(self.remain_size > 0 && self.remain_size <= u16::MAX as i32);
        result.put_u16(self.remain_size as u16);

        result.put_u32(self.right_pid);

        let mut byte_bar_offset = BTreePageDelegate::HEADER_SIZE as u32;
        let mut bottom_offset = self.page_size.get() as u32;

        for item in &self.content {
            let mut bytes = Vec::new();
            item.write_bytes(&mut bytes)?;
            let bytes_size = bytes.len() as u16;

            result.seek(byte_bar_offset);
            result.put_u16(bytes_size);

            bottom_offset -= bytes_size as u32;

            result.seek(bottom_offset as u32);
            result.put(&bytes);

            byte_bar_offset += 2;
        }

        Ok(result)
    }

    pub fn insert(&mut self, index: usize, item: BTreeDataItem) {
        let mut bytes = Vec::<u8>::new();
        item.write_bytes(&mut bytes).unwrap();

        let byte_len = bytes.len() as i32;

        self.remain_size -= byte_len + 2;

        self.content.insert(index, item);
    }

    pub fn remove(&mut self, index: usize) {
        let item = self.content.remove(index);

        let mut bytes = Vec::<u8>::new();
        item.write_bytes(&mut bytes).unwrap();

        let byte_len = bytes.len() as i32;

        self.remain_size += byte_len + 2;
    }

    pub fn get(&self, index: usize) -> &BTreeDataItem {
        &self.content[index]
    }

    #[inline]
    pub fn remain_size(&self) -> i32 {
        self.remain_size
    }

    pub fn len(&self) -> usize {
        self.content.len()
    }

    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }

    #[inline]
    pub fn storage_size(&self) -> usize {
        self.page_size.get() as usize - BTreePageDelegate::HEADER_SIZE
    }

    #[inline]
    pub fn set_right_pid(&mut self, right_pid: u32) {
        self.right_pid = right_pid;
    }
}

#[derive(Clone)]
pub(crate) struct BTreeDataItemWithKey {
    pub left_pid:   u32,
    pub key:        Bson,
    pub key_data_ticket: Option<DataTicket>,  // is Some() only if key is bigger than 254
    pub payload:    DataTicket,
}

impl BTreeDataItemWithKey {

    fn from_item(item: &BTreeDataItem, session: &dyn Session) -> DbResult<BTreeDataItemWithKey> {
        let (key_bytes, key_data_ticket) = if item.key_len == 255 {
            let data_ticket = DataTicket::from_bytes(&item.key_content);
            let key_bytes = session.get_data_from_storage(&data_ticket)?;
            (key_bytes, Some(data_ticket))
        } else {
            (item.key_content.clone(), None)
        };

        let key = deserialize_key_with_ty(item.key_ty, &key_bytes)?;

        Ok(BTreeDataItemWithKey {
            left_pid: item.left_pid,
            key,
            key_data_ticket,
            payload: item.payload.clone(),
        })
    }

}

pub(crate) struct BTreePageDelegateWithKey {
    page_id:       u32,
    parent_id:     u32,
    page_size:     NonZeroU32,
    remain_size:   i32,  // can be negative
    pub right_pid: u32,
    pub content:   Vec<BTreeDataItemWithKey>,
}

pub(crate) struct PageDivisionResult {
    pub left: BTreePageDelegateWithKey,
    pub right: BTreePageDelegateWithKey,
    pub middle_item: BTreeDataItemWithKey,
}

impl BTreePageDelegateWithKey {

    #[inline]
    pub fn page_id(&self) -> u32 {
        self.page_id
    }

    #[inline]
    pub fn parent_id(&self) -> u32 {
        self.parent_id
    }

    #[inline]
    pub fn page_size(&self) -> NonZeroU32 {
        self.page_size
    }

    pub fn read_from_session(base: BTreePageDelegate, session: &dyn Session) -> DbResult<BTreePageDelegateWithKey> {
        let mut content = Vec::new();

        for item in &base.content {
            let item_with_key = BTreeDataItemWithKey::from_item(item, session)?;
            content.push(item_with_key);
        }

        Ok(BTreePageDelegateWithKey {
            page_id: base.page_id,
            parent_id: base.parent_id,
            page_size: base.page_size,
            remain_size: base.remain_size,
            right_pid: base.right_pid,
            content,
        })
    }

    pub fn search(&self, key: &Bson) -> DbResult<SearchKeyResult> {
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

    pub fn insert(&mut self, _index: usize, _item: BTreeDataItemWithKey) {
        unimplemented!()
    }

    pub fn update_content(&mut self, _index: usize, _item: BTreeDataItemWithKey) {
        // make sure the size of the key is the same
        unimplemented!()
    }

    pub fn divide_in_the_middle(&self, session: &dyn Session, right_page_pid: u32) -> DbResult<PageDivisionResult> {
        let middle_index = self.len() / 2;
        let middle_item = self.content[middle_index].clone();

        // use current page block to store left
        let left = {
            let left_base = BTreePageDelegate {
                page_id: self.page_id,
                parent_id: self.parent_id,
                page_size: self.page_size,
                remain_size: (self.page_size.get() - (BTreePageDelegate::HEADER_SIZE as u32)) as i32,
                right_pid: 0,
                content: vec![],
            };

            let mut left_delegate = BTreePageDelegateWithKey::read_from_session(left_base, session)?;

            let mut index: usize = 0;
            for item in &self.content[0..middle_index] {
                left_delegate.insert(0, item.clone());
                index += 1;
            }

            left_delegate.set_right_pid(index - 1, middle_item.left_pid);

            left_delegate
        };

        // alloc new page to store right
        let right = {
            let right_page = BTreePageDelegate {
                page_id: right_page_pid,
                parent_id: self.parent_id,
                page_size: self.page_size,
                remain_size: (self.page_size.get() - (BTreePageDelegate::HEADER_SIZE as u32)) as i32,
                right_pid: 0,
                content: vec![],
            };
            let mut right_delegate = BTreePageDelegateWithKey::read_from_session(right_page, session)?;

            let mut index: usize = 0;
            for item in &self.content[(middle_index + 1)..] {
                right_delegate.insert(0, item.clone());
                index += 1;
            }

            right_delegate.set_right_pid(index - 1, self.right_pid);

            right_delegate
        };

        Ok(PageDivisionResult {
            left,
            right,
            middle_item,
        })
    }

    pub fn set_right_pid(&mut self, index: usize, right_pid: u32) {
        if index == self.content.len() - 1 {
            self.right_pid = right_pid;
            return;
        }

        // The left-pid of the next item is the right pid.
        self.content[index + 1].left_pid = right_pid;
    }

    pub fn get_right_pid(&self, index: usize) -> u32 {
        if index == self.content.len() - 1 {
            return self.right_pid;
        }
        self.content[index + 1].left_pid
    }

    pub fn get_left_pid(&self, index: usize) -> u32 {
        self.content[index].left_pid
    }

    pub fn merge_with_center(
        page_id: u32,
        parent_id: u32,
        page_size: NonZeroU32,
        left: &BTreePageDelegateWithKey,
        right: &BTreePageDelegateWithKey,
        mut center: BTreeDataItemWithKey,
    ) -> DbResult<BTreePageDelegateWithKey> {
        let remain_size: i32 = page_size.get() as i32 - (BTreePageDelegate::HEADER_SIZE as i32);
        let mut result = BTreePageDelegateWithKey {
            page_id,
            parent_id,
            page_size,
            remain_size,
            right_pid: 0,
            content: Vec::with_capacity(left.content.len() + right.content.len() + 1),
        };

        for item in &left.content {
            result.content.push(item.clone());
        }

        center.left_pid = left.right_pid;
        result.content.push(center);

        for item in &right.content {
            result.content.push(item.clone());
        }
        result.right_pid = right.right_pid;

        Ok(result)
    }

    pub fn shift_head(&mut self) -> BTreeDataItemWithKey {
        if self.content.is_empty() {
            panic!("btree content is empty, pid: {}", self.page_id);
        }

        let first_content  = self.content[0].clone();

        self.content.remove(0);

        first_content
    }

    pub fn shift_last(&mut self) -> (BTreeDataItemWithKey, u32) {
        if self.content.is_empty() {
            panic!("btree content is empty, pid: {}", self.page_id);
        }

        let last_index = self.right_pid;
        let last_content = self.content.last().unwrap().clone();

        self.content.remove(self.content.len() - 1);

        (last_content, last_index)
    }

    pub fn insert_head(&mut self, item: BTreeDataItemWithKey) {
        self.content.insert(0, item);
    }

    pub fn insert_back(&mut self, mut item: BTreeDataItemWithKey, right_pid: u32) {
        item.left_pid = self.right_pid;
        self.content.push(item);
        self.right_pid = right_pid;
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.content.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }

    #[inline]
    pub fn remain_size(&self) -> i32 {
        self.remain_size
    }

    pub fn generate_page(&self) -> DbResult<RawPage> {
        assert!(self.remain_size >= 0);
        let mut delegate = BTreePageDelegate {
            page_id: self.page_id,
            parent_id: self.parent_id,
            page_size: self.page_size,
            remain_size: self.remain_size,
            right_pid: self.right_pid,
            content: Vec::with_capacity(self.content.len()),
        };

        for item in &self.content {
            let mut key_bytes = Vec::<u8>::new();

            serialize_key(&item.key, &mut key_bytes)?;

            let item_without_key = BTreeDataItem {
                left_pid: item.left_pid,
                key_ty: item.key.element_type() as u8,
                key_len: 0,
                key_content: key_bytes,
                payload: item.payload.clone(),
            };

            delegate.content.push(item_without_key);
        }

        delegate.generate_page()
    }

    #[inline]
    pub(crate) fn is_leaf(&self) -> bool {
        self.content[0].left_pid == 0
    }
}

pub fn serialize_key<W: Write>(key: &Bson, writer: &mut W) -> DbResult<()> {
    match key {
        Bson::ObjectId(oid) => {
            // 12 bytes for key content
            let bytes = oid.bytes();
            writer.write_all(&bytes)?;

            Ok(())
        }

        Bson::Boolean(bl) => {
            writer.write_u8(if *bl {
                1
            } else {
                0
            })?;

            Ok(())
        }

        Bson::Int32(int) => {
            vli::encode(writer, *int as i64)?;
            Ok(())
        }

        Bson::Int64(int) => {
            vli::encode(writer, *int as i64)?;
            Ok(())
        }

        Bson::String(str) => {
            writer.write_all(str.as_bytes())?;
            Ok(())
        }

        _ => {
            let name = format!("{:?}", key);
            Err(DbErr::NotAValidKeyType(name))
        }
    }
}

pub fn deserialize_key_with_ty(key_ty: u8, buffer: &[u8]) -> DbResult<Bson> {
    let element_type = ElementType::from(key_ty);

    let value = match element_type {
        Some(ElementType::ObjectId) => {
            let mut oid_bytes = [0; 12];
            oid_bytes.copy_from_slice(buffer);
            let oid = ObjectId::from(oid_bytes);
            oid.into()
        }

        Some(ElementType::Boolean) => {
            let bl_value = buffer[0] != 0;
            Bson::Boolean(bl_value)
        }

        Some(ElementType::Int32) => {
            let (int_value, _) = vli::decode_u64(buffer)?;
            Bson::Int32(int_value as i32)
        }

        Some(ElementType::Int64) => {
            let (int_value, _) = vli::decode_u64(buffer)?;
            Bson::Int64(int_value as i64)
        }

        Some(ElementType::String) => {
            let str = String::from_utf8(buffer.to_vec()).unwrap();
            Bson::String(str)
        }

        _ => {
            let error_msg = format!("type {} is not suitable for _id", key_ty);
            return Err(DbErr::ParseError(error_msg));
        }
    };
    Ok(value)
}

