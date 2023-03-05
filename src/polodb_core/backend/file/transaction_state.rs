/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use crate::data_structures::trans_map::{TransMap, TransMapDraft};
use crate::transaction::TransactionType;

pub(super) struct TransactionState {
    pub(super) ty: TransactionType,
    pub(super) offset_map: TransMapDraft<u32, u64>,
    pub(super) frame_count: u32,
    pub(super) db_file_size: u64,
}

impl TransactionState {

    pub(super) fn new(ty: TransactionType, offset_map: TransMap<u32, u64>, frame_count: u32, db_file_size: u64) -> TransactionState {
        TransactionState {
            ty,
            offset_map: TransMapDraft::new(offset_map),
            frame_count,
            db_file_size,
        }
    }

    #[inline]
    pub fn set_type(&mut self, ty: TransactionType) {
        self.ty = ty;
    }

}
