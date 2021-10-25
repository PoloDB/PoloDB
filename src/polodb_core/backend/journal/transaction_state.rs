use std::collections::BTreeMap;
use crate::transaction::TransactionType;

pub(crate) struct TransactionState {
    pub(crate) ty: TransactionType,
    pub(crate) offset_map: BTreeMap<u32, u64>,
    pub(crate) frame_count: u32,
    pub(crate) db_file_size: u64,
}

impl TransactionState {

    pub(crate) fn new(ty: TransactionType, frame_count: u32, db_file_size: u64) -> TransactionState {
        TransactionState {
            ty,
            offset_map: BTreeMap::new(),
            frame_count,
            db_file_size,
        }
    }

}
