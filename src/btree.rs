use std::sync::Arc;
use std::sync::Weak;
use std::cmp::Ordering;

use super::bson::ObjectId;
use crate::db::{DbContext, DbResult};
use crate::page::RawPage;
use crate::error::DbErr;
use crate::serialization::DbSerializer;

static HEADER_SIZE: u32      = 64;
static ITEM_SIZE: u32        = 500;
static ITEM_HEADER_SIZE: u32 = 20;

#[derive(Clone)]
struct BTreeItem {
    parent_pid:   u32,
    left_pid:     u32,
    right_pid:    u32,
    object_id:    ObjectId,
    overflow_pid: u32,
    data:         Vec<u8>,
}

struct BTreeNode {
    parent_pid:  u32,
    pid:         u32,
    content:     Vec<BTreeItem>,
}

impl BTreeNode {

    // Offset 0: magic(2 bytes)
    // Offset 2: items_len(2 bytes)
    // Offset 4: left_pid (4 bytes)
    // Offset 8: next_pid (4 bytes)
    fn from_raw(page: &RawPage, pid: u32, parent_pid: u32, item_size: u32) -> DbResult<BTreeNode> {
        let mut left_pid = page.get_u32(4);
        let mut result = vec![];

        for i in 0..item_size {
            let offset: u32 = HEADER_SIZE + i * ITEM_SIZE;

            let right_pid = page.get_u32(offset);
            let oid_offset: usize = (offset + 4) as usize;
            let oid = ObjectId::deserialize(&page.data[oid_offset..(oid_offset + 12)])?;

            let overflow_pid = page.get_u32(16);

            let data_offset: usize = (offset + ITEM_HEADER_SIZE) as usize;

            let data = page.data[data_offset..(data_offset + ((ITEM_SIZE - ITEM_HEADER_SIZE) as usize))].to_vec();

            result.push(BTreeItem {
                parent_pid,
                left_pid, right_pid,
                object_id: oid,
                overflow_pid,
                data,
            });

            left_pid = right_pid;
        }

        Ok(BTreeNode {
            pid,
            parent_pid,
            content: result,
        })
    }

    fn to_raw(&self, page: &mut RawPage) -> DbResult<()> {
        let items_len = page.data.len() as u16;

        page.seek(2);
        page.put_u16(items_len);

        self.content.first().map(|first| {
            page.seek(4);
            page.put_u32(first.left_pid);
        });

        for (index, item) in self.content.iter().enumerate() {
            let offset: u32 = HEADER_SIZE + (index as u32) * ITEM_SIZE;

            page.seek(offset);
            page.put_u32(item.right_pid);

            let mut oid_bytes: Vec<u8> = Vec::with_capacity(12);
            item.object_id.serialize(&mut oid_bytes)?;

            page.seek(offset + 4);
            page.put(&oid_bytes);

            // TODO: overflow pid

            page.seek(offset + ITEM_HEADER_SIZE);
            page.put(&item.data);
        }

        Ok(())
    }

    fn is_root(&self) -> bool {
        self.parent_pid == 0
    }

}

// Offset 0:  header(64 bytes)
// Offset 64: Item(500 bytes) * 8
//
// Item struct:
// Offset 0: right pid(4 bytes)
// Offset 4: object_id(12 bytes)
// Offset 16: overflow_pid(4 bytes)
// Offset 20: data
struct BTreePageWrapper {
    ctx:                Weak<DbContext>,
    root_page_id:       u32,
    item_size:          u32,
}

impl BTreePageWrapper {

    pub fn new(ctx: Arc<DbContext>, root_page_id: u32) -> BTreePageWrapper {
        let item_size = (ctx.as_ref().page_size - HEADER_SIZE) / ITEM_SIZE;

        BTreePageWrapper {
            ctx: Arc::downgrade(&ctx),
            root_page_id, item_size
        }
    }

    fn insert_item(&mut self, oid: &ObjectId, data: &[u8], replace: bool) -> DbResult<()> {
        // insert to root node
        self.insert_item_to_page(self.root_page_id, 0, oid, data, replace)
    }

    fn get_node(&self, pid: u32, parent_pid: u32) -> DbResult<BTreeNode> {
        let mut ctx_rc = self.ctx.upgrade().expect("context missing");
        let mut ctx = Arc::get_mut(&mut ctx_rc).expect("get mut error");

        let raw_page = ctx.pipeline_read_page(pid)?;

        BTreeNode::from_raw(&raw_page, pid, parent_pid, self.item_size)
    }

    fn insert_item_to_page(&mut self, pid: u32, parent_pid: u32, oid: &ObjectId, data: &[u8], replace: bool) -> DbResult<()> {
        if data.len() > (ITEM_SIZE - ITEM_HEADER_SIZE) as usize {
            return Err(DbErr::DataOverflow);
        }

        let mut btree_node: BTreeNode = self.get_node(pid, parent_pid)?;

        let mut index: usize = 0;

        // if oid < data_content[index].object_id {
        //     // insert in left page
        // }

        while index < (self.item_size as usize) {
            let target = &btree_node.content[index];
            let cmp_result = oid.cmp(&target.object_id);

            match cmp_result {
                Ordering::Equal => {
                    // replace if should
                    break;
                }

                Ordering::Less => {
                    if target.left_pid == 0 {  // left is null, insert in current page
                        let new_item = BTreeItem {
                            parent_pid: pid,
                            left_pid: 0,
                            right_pid: 0,
                            object_id: oid.clone(),
                            overflow_pid: 0,
                            data: data.to_vec(),
                        };

                        // insert between index - 1 and index
                        btree_node.content.insert(index, new_item);
                        break;
                    } else {  // left has page
                        // insert to left page
                        return self.insert_item_to_page(target.left_pid, pid, oid, data, replace);
                    }
                }

                Ordering::Greater => () // next iter
            }

            index += 1;
        }

        if index >= btree_node.content.len() {  // greater than the last
            let last: &BTreeItem = btree_node.content.last().expect("should has last one");
            if last.right_pid == 0 {  // right page is null, insert in current page
                let new_item = BTreeItem {
                    parent_pid: pid,
                    left_pid: 0,
                    right_pid: 0,
                    object_id: oid.clone(),
                    overflow_pid: 0,
                    data: data.to_vec(),
                };

                btree_node.content.insert(btree_node.content.len(), new_item);
            } else {  // insert to right page
                return self.insert_item_to_page(last.right_pid, pid, oid, data, replace);
            }
        }

        if btree_node.content.len() > (self.item_size as usize) {  // need to divide
            return self.divide_up(btree_node)
        }

        // write page back
        self.write_btree_node(&btree_node)
    }

    fn write_btree_node(&mut self, node: &BTreeNode) -> DbResult<()> {
        let mut ctx_rc = self.ctx.upgrade().expect("context missing");
        let mut ctx = Arc::get_mut(&mut ctx_rc).expect("get mut error");
        let mut raw_page = RawPage::new(node.pid, ctx.page_size);

        node.to_raw(&mut raw_page)?;

        ctx.pipeline_write_page(&raw_page)
    }

    fn divide_up(&mut self, btree_node: BTreeNode) -> DbResult<()> {
        let mut ctx_rc = self.ctx.upgrade().expect("context missing");
        let mut ctx = Arc::get_mut(&mut ctx_rc).expect("get mut error");

        if btree_node.is_root() {
            let middle_index = (btree_node.content.len() + 1) / 2;

            let left_page_id = ctx.alloc_page_id()?;
            let right_page_id = ctx.alloc_page_id()?;

            let left = BTreeNode {
                parent_pid:  btree_node.pid,
                pid:         left_page_id,
                content:     {
                    let mut result = vec![];

                    for i in 0..middle_index {
                        result.push(btree_node.content[i].clone());
                    }

                    result
                },
            };

            let right = BTreeNode {
                parent_pid:  btree_node.pid,
                pid:         right_page_id,
                content:     {
                    let mut result = vec![];

                    for i in (middle_index + 1)..btree_node.content.len() {
                        result.push(btree_node.content[i].clone());
                    }

                    result
                },
            };

            self.write_btree_node(&left)?;
            self.write_btree_node(&right)?;

            let middle = btree_node.content[middle_index].clone();
            let mut btree_node = btree_node;
            btree_node.content.push(middle);

            self.write_btree_node(&btree_node)
        } else {
            Err(DbErr::NotImplement)
        }
    }

}
