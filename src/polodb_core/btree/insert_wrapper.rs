/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use bson::Document;
use crate::btree::btree_v2::{BTreeDataItemWithKey, BTreePageDelegate, BTreePageDelegateWithKey, serialize_key};
use crate::DbResult;
use crate::page::RawPage;
use crate::error::DbErr;
use crate::data_ticket::DataTicket;
use crate::session::Session;
use super::SearchKeyResult;
use super::wrapper_base::BTreePageWrapperBase;

pub(crate) struct InsertBackwardItem {
    pub content: BTreeDataItemWithKey,
    pub right_pid: u32,
}

pub(crate) struct InsertResult {
    pub backward_item: Option<InsertBackwardItem>,
}

impl InsertBackwardItem {

    pub(crate) fn write_to_page(&self, session: &dyn Session, new_page_id: u32, left_pid: u32) -> DbResult<RawPage> {
        let page_size = session.page_size();
        let result = RawPage::new(new_page_id, page_size);

        let delegate = BTreePageDelegate::from_page(&result, 0)?;
        let mut delegate_with_key = BTreePageDelegateWithKey::read_from_session(
            delegate, session,
        )?;

        let mut item = self.content.clone();
        item.left_pid = left_pid;

        delegate_with_key.insert(0, item);
        delegate_with_key.set_right_pid(0, self.right_pid);

        let result = delegate_with_key.generate_page()?;
        Ok(result)
    }

}

mod doc_validation {
    use bson::Document;
    use crate::{DbResult, DbErr};

    fn validate_key(key: &str) -> DbResult<()> {
        let mut i: usize = 0;
        while i < key.len() {
            let ch = key.chars().nth(i).unwrap();
            match ch {
                ' ' | '$' | '.' |
                '<' | '>' | '[' |
                ']' | '{' | '}' => {
                    let msg = format!("illegal key content: '{}'", key);
                    return Err(DbErr::ValidationError(msg))
                }

                _ => {
                    i += 1;
                }
            }
        }
        Ok(())
    }

    pub(super) fn validate(doc: &Document) -> DbResult<()> {
        for (key, _value) in doc.iter() {
            validate_key(key.as_ref())?;
        }
        Ok(())
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

    pub(crate) fn new(page_handler: &dyn Session, root_page_id: u32) -> BTreePageInsertWrapper {
        let base = BTreePageWrapperBase::new(page_handler, root_page_id);
        BTreePageInsertWrapper(base)
    }

    pub(crate) fn insert_item(&mut self, doc: &Document, replace: bool) -> DbResult<InsertResult> {
        doc_validation::validate(doc)?;
        // insert to root node
        self.insert_item_to_page(self.0.root_page_id, 0, doc, false, replace)
    }

    #[inline]
    fn store_doc(&mut self, doc: &Document) -> DbResult<DataTicket> {
        self.0.session.store_doc(doc)
    }

    fn store_doc_as_payload(&mut self, left_pid: u32, doc: &Document) -> DbResult<BTreeDataItemWithKey> {
        let pkey = doc.get("_id").unwrap();

        let payload = self.store_doc(doc)?;

        let mut key_bytes: Vec<u8> = Vec::new();
        serialize_key(pkey, &mut key_bytes)?;

        let key_data_ticket = if key_bytes.len() >= 255 {
            let data_ticket = self.0.session.store_data_in_storage(&key_bytes)?;
            Some(data_ticket)
        } else {
            None
        };

        Ok(BTreeDataItemWithKey {
            left_pid,
            key: pkey.clone(),
            key_data_ticket,
            payload,
        })
    }

    // fn doc_to_node_data_item(&mut self, left_pid: u32, doc: &Document) -> DbResult<BTreeDataItem> {
    //     let pkey = doc.get("_id").unwrap();
    //
    //     let mut key_bytes = Vec::<u8>::new();
    //     serialize_key(pkey, &mut key_bytes)?;
    //
    //     let (key_len, key_content) = if key_bytes.len() > 254 {
    //         let key_data_ticket = self.0.session.store_data_in_storage(&key_bytes)?;
    //         let key_bytes = key_data_ticket.to_bytes().to_vec();
    //         (255 as u8, key_bytes)
    //     } else {
    //         (key_bytes.len() as u8, key_bytes)
    //     };
    //
    //     let key_ty = pkey.element_type() as u8;
    //
    //     let payload = self.store_doc(doc)?;
    //
    //     Ok(BTreeDataItem {
    //         left_pid,
    //         key_ty,
    //         key_len,
    //         key_content,
    //         payload,
    //     })
    // }

    pub(crate) fn insert_item_to_page(
        &mut self,
        pid: u32,
        parent_pid: u32,
        doc: &Document,
        backward: bool,
        replace: bool,
    ) -> DbResult<InsertResult> {
        let mut btree_node = self.0.get_node(pid, parent_pid)?;

        if btree_node.is_empty() {
            let data_item = self.store_doc_as_payload(0, doc)?;
            btree_node.insert(0, data_item);

            self.0.write_btree_node(&btree_node)?;

            return Ok(InsertResult {
                backward_item: None,
            });
        }

        // let mut index: usize = 0;
        let doc_pkey = &doc.get("_id").expect("primary key not found in document").into();

        let search_result = btree_node.search(doc_pkey)?;
        match search_result {
            SearchKeyResult::Node(index) => {
                return if replace {
                    let original_left_pid = btree_node.get_left_pid(index);
                    let data_item = self.store_doc_as_payload(original_left_pid, doc)?;

                    btree_node.update_content(index, data_item);

                    self.0.write_btree_node(&btree_node)?;

                    Ok(InsertResult {
                        backward_item: None,
                    })
                } else {
                    let str = format!("data exist: {}", doc_pkey);
                    Err(DbErr::DataExist(str))
                }
            }

            SearchKeyResult::Index(index) => {
                let left_pid = btree_node.get_left_pid(index);
                if backward || left_pid == 0 {  // left is null, insert in current page
                    // insert between index - 1 and index
                    let data_item = self.store_doc_as_payload(0, doc)?;
                    btree_node.insert(index, data_item);
                } else {  // left has page
                    // insert to left page
                    let tmp = self.insert_item_to_page(left_pid, pid, doc, false, replace)?;
                    if let Some(backward_item) = tmp.backward_item {
                        btree_node.insert(index, backward_item.content);
                        btree_node.set_right_pid(index, backward_item.right_pid);
                    }
                }
            }

        };

        if btree_node.remain_size() < 0 {  // need to divide
            return self.divide_and_return_backward(btree_node);
        }

        // write page back
        self.0.write_btree_node(&btree_node)?;

        Ok(InsertResult {
            backward_item: None,
        })
    }

    fn divide_and_return_backward(&mut self, btree_page_delegate: BTreePageDelegateWithKey) -> DbResult<InsertResult> {
        let right_page_id = self.0.session.alloc_page_id()?;

        let result = btree_page_delegate.divide_in_the_middle(
            self.0.session, right_page_id,
        )?;

        self.0.write_btree_node(&result.left)?;
        self.0.write_btree_node(&result.right)?;

        let backward_item = InsertBackwardItem {
            content: result.middle_item,
            right_pid: right_page_id,
        };

        Ok(InsertResult {
            backward_item: Some(backward_item),
        })
    }

}

