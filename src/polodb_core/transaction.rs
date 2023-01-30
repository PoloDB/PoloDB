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
    UserAuto,
    DbAuto(Cell<i32>),
}

impl TransactionState {

    pub(crate) fn new_db_auto() -> TransactionState {
        TransactionState::DbAuto(Cell::new(1))
    }

    #[inline]
    pub(crate) fn is_no_trans(&self) -> bool {
        if let TransactionState::NoTrans = self {
            true
        } else {
            false
        }
    }

    pub(crate) fn acquire(&self) {
        if let TransactionState::DbAuto(counter) = self {
            counter.set(counter.get() + 1)
        }
    }

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
