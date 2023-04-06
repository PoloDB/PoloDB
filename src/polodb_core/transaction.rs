/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::cell::Cell;
use serde::{Serialize, Deserialize};

#[derive(Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
#[repr(u8)]
pub enum TransactionType {
    Read = 1,
    Write = 2,
}

#[derive(Eq, PartialEq, Clone)]
pub(crate) enum TransactionState {
    NoTrans,
    User,
    DbAuto(Cell<i32>),
}

impl TransactionState {

    #[inline]
    pub(crate) fn new_db_auto() -> TransactionState {
        TransactionState::DbAuto(Cell::new(1))
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn is_no_trans(&self) -> bool {
        if let TransactionState::NoTrans = self {
            true
        } else {
            false
        }
    }

    #[allow(dead_code)]
    pub(crate) fn acquire(&self) {
        if let TransactionState::DbAuto(counter) = self {
            counter.set(counter.get() + 1)
        }
    }

    #[allow(dead_code)]
    pub (crate) fn release(&self) -> bool {
        if let TransactionState::DbAuto(counter) = self {
            counter.set(counter.get() - 1);
            let value = counter.get();
            value == 0
        } else {
            false
        }
    }

}
