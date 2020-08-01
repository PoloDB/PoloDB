
//         std::int64_t left_pid = 0;  // 8 bytes
//         std::uint16_t key_size = 0;  // 2 bytes
//         Handle<ObjectId> key;  // 12 bytes
//         std::uint16_t data_size = 0;  // 2 bytes
//         unsigned char* data = nullptr;  // max 468 bytes
//         std::int64_t overflow_pid = 0;  // 8 bytes

use super::bson::ObjectId;
use std::sync::Arc;
use std::sync::Weak;

#[derive(Debug)]
struct BtreeEntry {
    left_pid:     i64,
    key_size:     u16,
    key:          ObjectId,
    data_size:    u16,
    data:         Vec<u8>,
    overflow_pid: i64,
}

#[derive(Debug)]
struct BTreeNode {
    entries:     Vec<BtreeEntry>,
    btree:       Weak<BTree>,
    parent:      Weak<BTreeNode>,
}

#[derive(Debug)]
struct BTree {
    root:       Arc<BTreeNode>,
}
